using System.Diagnostics;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Net.Sockets;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;
using Beskar.Utilities.Console.Rendering;

namespace Beskar.Networking.Performance;

public static class Program
{
   private static int GetFreePort()
   {
      using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
      return ((IPEndPoint)socket.LocalEndPoint!).Port;
   }

   public static async Task Main(string[] args)
   {
      ConsoleRender.DrawHeader("Beskar Performance Shootout", "High-Performance Network Transport Shootout Benchmark",
         BoxStyle.Double, ConsoleColor.Cyan);
      ConsoleRender.WriteMarkupLine("Welcome to the high-performance network transport shootout benchmark!");
      ConsoleRender.WriteMarkupLine(
         "This utility measures the maximum raw throughput over local loopback ([green]127.0.0.1[/]).\n");

      var choices = new[]
      {
         new PromptChoice("a", "Run shootout shootout (All Transports)"),
         new PromptChoice("t", "Test TCP throughput"),
         new PromptChoice("w", "Test WebSocket throughput"),
         new PromptChoice("q", "Test QUIC throughput"),
         new PromptChoice("x", "Exit Shootout")
      };

      var selectedOption = ConsoleRender.AskChoice("Select action", choices, "a", true);
      if (selectedOption == "x")
      {
         ConsoleRender.WriteMarkupLine("[yellow]Goodbye![/]");
         return;
      }

      var durationStr = ConsoleRender.AskString("Enter test duration per transport (seconds)", "3");
      if (!int.TryParse(durationStr, out var durationSec) || durationSec <= 0) durationSec = 3;

      var payloadKbStr = ConsoleRender.AskString("Enter payload block size (KB)", "64");
      if (!int.TryParse(payloadKbStr, out var payloadKb) || payloadKb <= 0) payloadKb = 64;
      var payloadSize = payloadKb * 1024;

      ConsoleRender.WriteMarkupLine(
         $"\n[yellow]Configuration finalized:[/] Duration: [green]{durationSec}s[/], Block Size: [green]{payloadKb} KB[/] ({payloadSize} bytes)\n");

      // Results placeholders
      BenchmarkResult? tcpResult = null;
      BenchmarkResult? wsResult = null;
      BenchmarkResult? quicResult = null;

      var runTcp = selectedOption is "a" or "t";
      var runWs = selectedOption is "a" or "w";
      var runQuic = selectedOption is "a" or "q";

      if (runTcp) tcpResult = await RunTcpBenchmarkAsync(durationSec, payloadSize);

      if (runWs) wsResult = await RunWsBenchmarkAsync(durationSec, payloadSize);

      if (runQuic)
      {
         if (QuicConnection.IsSupported)
            quicResult = await RunQuicBenchmarkAsync(durationSec, payloadSize);
         else
            ConsoleRender.WriteMarkupLine("[red]QUIC is not supported on this platform. Skipping QUIC benchmark.[/]");
      }

      // Print comparative table
      ConsoleRender.WriteMarkupLine("\n[cyan]=== BENCHMARK SHOOTOUT RESULTS ===[/]");
      var table = ConsoleRender.CreateTable()
         .SetStyle(BoxStyle.Double)
         .SetBorderColor(ConsoleColor.Cyan)
         .AddColumn("Transport", Alignment.Left, ConsoleColor.Yellow)
         .AddColumn("Transferred (GB)", Alignment.Right, ConsoleColor.Green)
         .AddColumn("Duration (s)", Alignment.Right, ConsoleColor.White)
         .AddColumn("Speed (MB/s)", Alignment.Right, ConsoleColor.Magenta)
         .AddColumn("Speed (Gbps)", Alignment.Right, ConsoleColor.Cyan)
         .AddColumn("Msg/s", Alignment.Right, ConsoleColor.Green);

      AddResultRow(table, "TCP", tcpResult, payloadSize);
      AddResultRow(table, "WebSocket", wsResult, payloadSize);
      AddResultRow(table, "QUIC", quicResult, payloadSize);

      table.Render();
      ConsoleRender.WriteMarkupLine("\n[green]Shootout successfully completed![/]");
   }

   private static void AddResultRow(ConsoleTable table, string transportName, BenchmarkResult? result, int payloadSize)
   {
      if (result is null)
      {
         table.AddRow(transportName, "[red]N/A[/]", "[red]N/A[/]", "[red]N/A[/]", "[red]N/A[/]", "[red]N/A[/]");
         return;
      }

      var totalGb = result.TotalBytesTransferred / (1000.0 * 1000.0 * 1000.0);
      var mbPerSec = result.TotalBytesTransferred / (1024.0 * 1024.0 * result.DurationSeconds);
      var gbps = result.TotalBytesTransferred * 8.0 / (1000.0 * 1000.0 * 1000.0 * result.DurationSeconds);
      var msgsPerSec = result.TotalBytesTransferred / (double)payloadSize / result.DurationSeconds;

      table.AddRow(
         transportName,
         totalGb.ToString("N3"),
         result.DurationSeconds.ToString("N2"),
         mbPerSec.ToString("N2"),
         gbps.ToString("N2"),
         msgsPerSec.ToString("N0")
      );
   }

   private static async Task<BenchmarkResult> RunTcpBenchmarkAsync(int durationSec, int payloadSize)
   {
      ConsoleRender.WriteMarkupLine("[yellow]Starting TCP throughput benchmark...[/]");

      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);
      var options = new TcpTransportOptions();

      var listener = new TcpNetworkListener(endPoint, options);
      var bindResult = await listener.BindAsync();
      if (bindResult.Failed)
      {
         ConsoleRender.WriteMarkupLine($"[red]TCP Bind failed:[/] {bindResult.Error.Message}");
         return new BenchmarkResult(0, durationSec);
      }

      var serverReadTaskCompletion = new TaskCompletionSource<long>();

      // Start Server Accept & Read loop in background
      _ = Task.Run(async () =>
      {
         var acceptResult = await listener.AcceptSessionAsync();
         if (acceptResult.Failed)
         {
            ConsoleRender.WriteMarkupLine($"[red]TCP Server AcceptSession failed:[/] {acceptResult.Error.Message}");
            serverReadTaskCompletion.SetResult(0);
            return;
         }

         var session = acceptResult.Success!;
         var streamResult = await session.AcceptStreamAsync();
         if (streamResult.Failed)
         {
            ConsoleRender.WriteMarkupLine($"[red]TCP Server AcceptStream failed:[/] {streamResult.Error.Message}");
            await session.DisposeAsync();
            serverReadTaskCompletion.SetResult(0);
            return;
         }

         var stream = streamResult.Success!;
         var reader = stream.Transport.Input;
         long bytesRead = 0;

         try
         {
            while (true)
            {
               var readResult = await reader.ReadAsync();
               var buffer = readResult.Buffer;
               bytesRead += buffer.Length;
               reader.AdvanceTo(buffer.End);

               if (readResult.IsCompleted) break;
            }
         }
         catch (Exception ex)
         {
            ConsoleRender.WriteMarkupLine($"[red]TCP Server Read loop exception:[/] {ex.Message}");
         }
         finally
         {
            await session.DisposeAsync();
            serverReadTaskCompletion.SetResult(bytesRead);
         }
      });

      // Client Connect & Flood
      var client = new TcpNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
      if (connectResult.Failed)
      {
         ConsoleRender.WriteMarkupLine($"[red]TCP Client connection failed:[/] {connectResult.Error.Message}");
         await listener.UnbindAsync();
         return new BenchmarkResult(0, durationSec);
      }

      var clientSession = connectResult.Success!;
      var clientStreamResult = await clientSession.OpenStreamAsync();
      var clientStream = clientStreamResult.Success!;

      var payload = new byte[payloadSize];
      Random.Shared.NextBytes(payload);

      var stopwatch = Stopwatch.StartNew();
      var targetTicks = TimeSpan.FromSeconds(durationSec).Ticks;

      while (stopwatch.ElapsedTicks < targetTicks)
      {
         await clientStream.Transport.Output.WriteAsync(payload);
         await clientStream.Transport.Output.FlushAsync();
      }

      stopwatch.Stop();

      // Complete write to signal client finished
      await clientStream.Transport.Output.CompleteAsync();
      await clientSession.DisposeAsync();

      // Wait for server to capture all bytes
      var totalBytesRead = await serverReadTaskCompletion.Task;
      await listener.UnbindAsync();

      ConsoleRender.WriteMarkupLine(
         $"[green]TCP Completed:[/] Transferred [white]{totalBytesRead / (1024.0 * 1024.0):N2} MB[/] in [white]{stopwatch.Elapsed.TotalSeconds:N2}s[/]\n");
      return new BenchmarkResult(totalBytesRead, stopwatch.Elapsed.TotalSeconds);
   }

   private static async Task<BenchmarkResult> RunWsBenchmarkAsync(int durationSec, int payloadSize)
   {
      ConsoleRender.WriteMarkupLine("[yellow]Starting WebSocket throughput benchmark...[/]");

      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);
      var options = new WsTransportOptions
      {
         Path = "/perf"
      };

      var listener = new WsNetworkListener(endPoint, options);
      var bindResult = await listener.BindAsync();
      if (bindResult.Failed)
      {
         ConsoleRender.WriteMarkupLine($"[red]WS Bind failed:[/] {bindResult.Error.Message}");
         return new BenchmarkResult(0, durationSec);
      }

      var serverReadTaskCompletion = new TaskCompletionSource<long>();

      // Start Server Accept & Read loop in background
      _ = Task.Run(async () =>
      {
         var acceptResult = await listener.AcceptSessionAsync();
         if (acceptResult.Failed)
         {
            ConsoleRender.WriteMarkupLine($"[red]WS Server AcceptSession failed:[/] {acceptResult.Error.Message}");
            serverReadTaskCompletion.SetResult(0);
            return;
         }

         var session = acceptResult.Success!;
         var streamResult = await session.AcceptStreamAsync();
         if (streamResult.Failed)
         {
            ConsoleRender.WriteMarkupLine($"[red]WS Server AcceptStream failed:[/] {streamResult.Error.Message}");
            await session.DisposeAsync();
            serverReadTaskCompletion.SetResult(0);
            return;
         }

         var stream = streamResult.Success!;
         var reader = stream.Transport.Input;
         long bytesRead = 0;

         try
         {
            while (true)
            {
               var readResult = await reader.ReadAsync();
               var buffer = readResult.Buffer;

               if (buffer.Length > 0)
               {
                  bytesRead += buffer.Length;
                  reader.AdvanceTo(buffer.End);
               }
               else
               {
                  reader.AdvanceTo(buffer.End);
               }

               if (readResult.IsCompleted) break;
            }
         }
         catch (Exception ex)
         {
            ConsoleRender.WriteMarkupLine($"[red]WS Server Read loop exception:[/] {ex.Message}");
         }
         finally
         {
            await session.DisposeAsync();
            serverReadTaskCompletion.SetResult(bytesRead);
         }
      });

      // Client Connect & Flood
      var client = new WsNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
      if (connectResult.Failed)
      {
         ConsoleRender.WriteMarkupLine($"[red]WS Client connection failed:[/] {connectResult.Error.Message}");
         await listener.UnbindAsync();
         return new BenchmarkResult(0, durationSec);
      }

      var clientSession = connectResult.Success!;
      var clientStreamResult = await clientSession.AcceptStreamAsync();
      var clientStream = clientStreamResult.Success!;

      var payload = new byte[payloadSize];
      Random.Shared.NextBytes(payload);

      var stopwatch = Stopwatch.StartNew();
      var targetTicks = TimeSpan.FromSeconds(durationSec).Ticks;

      while (stopwatch.ElapsedTicks < targetTicks)
      {
         await clientStream.Transport.Output.WriteAsync(payload);
         await clientStream.Transport.Output.FlushAsync();
      }

      stopwatch.Stop();

      // Signal complete
      await clientStream.Transport.Output.CompleteAsync();
      await clientSession.DisposeAsync();

      // Wait for server to capture all bytes
      var totalBytesRead = await serverReadTaskCompletion.Task;
      await listener.UnbindAsync();

      ConsoleRender.WriteMarkupLine(
         $"[green]WebSocket Completed:[/] Transferred [white]{totalBytesRead / (1024.0 * 1024.0):N2} MB[/] in [white]{stopwatch.Elapsed.TotalSeconds:N2}s[/]\n");
      return new BenchmarkResult(totalBytesRead, stopwatch.Elapsed.TotalSeconds);
   }

   private static async Task<BenchmarkResult> RunQuicBenchmarkAsync(int durationSec, int payloadSize)
   {
      ConsoleRender.WriteMarkupLine("[yellow]Starting QUIC throughput benchmark...[/]");

      var port = GetFreePort();
      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      using var certificate = CertificateUtility.GenerateSelfSignedCertificate();
      var serverSslOptions = new SslServerAuthenticationOptions
      {
         ServerCertificate = certificate,
         ClientCertificateRequired = false
      };

      var clientSslOptions = new SslClientAuthenticationOptions
      {
         RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
      };

      var options = new QuicTransportOptions
      {
         AlpnProtocol = "beskar-quic-perf",
         SslServerOptions = serverSslOptions,
         SslClientOptions = clientSslOptions
      };

      var listener = new QuicNetworkListener(endPoint, options);
      var bindResult = await listener.BindAsync();
      if (bindResult.Failed)
      {
         ConsoleRender.WriteMarkupLine($"[red]QUIC Bind failed:[/] {bindResult.Error.Message}");
         return new BenchmarkResult(0, durationSec);
      }

      var serverReadTaskCompletion = new TaskCompletionSource<long>();

      // Start Server Accept & Read loop in background
      _ = Task.Run(async () =>
      {
         var acceptResult = await listener.AcceptSessionAsync();
         if (acceptResult.Failed)
         {
            ConsoleRender.WriteMarkupLine($"[red]QUIC Server AcceptSession failed:[/] {acceptResult.Error.Message}");
            serverReadTaskCompletion.SetResult(0);
            return;
         }

         var session = acceptResult.Success!;
         var streamResult = await session.AcceptStreamAsync();
         if (streamResult.Failed)
         {
            ConsoleRender.WriteMarkupLine($"[red]QUIC Server AcceptStream failed:[/] {streamResult.Error.Message}");
            await session.DisposeAsync();
            serverReadTaskCompletion.SetResult(0);
            return;
         }

         var stream = streamResult.Success!;
         var reader = stream.Transport.Input;
         long bytesRead = 0;

         try
         {
            while (true)
            {
               var readResult = await reader.ReadAsync();
               var buffer = readResult.Buffer;
               bytesRead += buffer.Length;
               reader.AdvanceTo(buffer.End);

               if (readResult.IsCompleted) break;
            }
         }
         catch (Exception ex)
         {
            ConsoleRender.WriteMarkupLine($"[red]QUIC Server Read loop exception:[/] {ex.Message}");
         }
         finally
         {
            await session.DisposeAsync();
            serverReadTaskCompletion.SetResult(bytesRead);
         }
      });

      // Client Connect & Flood
      var client = new QuicNetworkClient(options);
      var connectResult = await client.ConnectAsync(endPoint);
      if (connectResult.Failed)
      {
         ConsoleRender.WriteMarkupLine($"[red]QUIC Client connection failed:[/] {connectResult.Error.Message}");
         await listener.UnbindAsync();
         return new BenchmarkResult(0, durationSec);
      }

      var clientSession = connectResult.Success!;

      // Open bidirectional stream
      var clientStreamResult = await clientSession.OpenStreamAsync();
      var clientStream = clientStreamResult.Success!;

      var payload = new byte[payloadSize];
      Random.Shared.NextBytes(payload);

      // Pre-write first payload to notify server of the QUIC stream
      await clientStream.Transport.Output.WriteAsync(payload);
      await clientStream.Transport.Output.FlushAsync();

      var stopwatch = Stopwatch.StartNew();
      var targetTicks = TimeSpan.FromSeconds(durationSec).Ticks;

      while (stopwatch.ElapsedTicks < targetTicks)
      {
         await clientStream.Transport.Output.WriteAsync(payload);
         await clientStream.Transport.Output.FlushAsync();
      }

      stopwatch.Stop();

      // Signal complete by disposing the stream wrapper (transmits FIN gracefully)
      await clientStream.DisposeAsync();

      // Wait for server to capture all bytes (including the pre-written one)
      var totalBytesRead = await serverReadTaskCompletion.Task;
      await clientSession.DisposeAsync();
      await listener.UnbindAsync();

      ConsoleRender.WriteMarkupLine(
         $"[green]QUIC Completed:[/] Transferred [white]{totalBytesRead / (1024.0 * 1024.0):N2} MB[/] in [white]{stopwatch.Elapsed.TotalSeconds:N2}s[/]\n");
      return new BenchmarkResult(totalBytesRead, stopwatch.Elapsed.TotalSeconds);
   }
}

public sealed class BenchmarkResult(long totalBytesTransferred, double durationSeconds)
{
   public long TotalBytesTransferred { get; } = totalBytesTransferred;
   public double DurationSeconds { get; } = durationSeconds;
}
