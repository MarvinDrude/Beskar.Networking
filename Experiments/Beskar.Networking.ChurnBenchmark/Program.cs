using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Benchmarks.Common;
using Beskar.Networking.Transports.Common.Options;
using Beskar.Networking.Transports.Memory;
using Beskar.Networking.Transports.NamedPipes;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Udp;
using Beskar.Networking.Transports.Uds;
using Beskar.Networking.Transports.Ws;
using Beskar.Utilities.Console.Rendering;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.ChurnBenchmark;

public static class Program
{
   public static async Task Main(string[] args)
   {
      TraceLogger.IsEnabled = false;

      ConsoleRender.DrawHeader("BESKAR TRANSPORT CONNECTION CHURN BENCHMARK",
         "High-churn benchmark for lower-level transport reconnects & cyclic message bursts");

      var transportChoice = 1;
      var concurrentClients = 10;
      var sendsPerConnection = 50;
      var payloadSize = 512;
      var durationSeconds = 15;
      var serverPort = 9200;

      if (args.Length >= 5)
      {
         int.TryParse(args[0], out transportChoice);
         int.TryParse(args[1], out concurrentClients);
         int.TryParse(args[2], out sendsPerConnection);
         int.TryParse(args[3], out payloadSize);
         int.TryParse(args[4], out durationSeconds);
      }
      else
      {
         Console.WriteLine("Select Transport Mode:");
         Console.WriteLine("  [1] TCP Transport");
         Console.WriteLine("  [2] WebSocket (WS) Transport");
         Console.WriteLine("  [3] QUIC Transport (System.Net.Quic / MsQuic)");
         Console.WriteLine("  [4] Named Pipes Transport");
         Console.WriteLine("  [5] Unix Domain Socket (UDS) Transport");
         Console.WriteLine("  [6] In-Memory Transport");
         Console.WriteLine("  [7] UDP Transport");
         Console.WriteLine("  [8] Run ALL Transports sequentially");
         Console.WriteLine();

         transportChoice = PromptInt("Transport choice", 1);
         concurrentClients = PromptInt("Concurrent clients (parallel workers)", 10);
         sendsPerConnection = PromptInt("Sends per connection before graceful reconnect", 50);
         payloadSize = PromptInt("Payload size per message (bytes)", 512);
         durationSeconds = PromptInt("Benchmark duration per transport (seconds)", 15);
         serverPort = PromptInt("Base server port", 9200);
      }

      Console.WriteLine();

      if (transportChoice == 8)
      {
         var results = new List<ChurnBenchmarkResult>();
         for (var mode = 1; mode <= 7; mode++)
         {
            var res = await RunBenchmarkForTransport(mode, concurrentClients, sendsPerConnection, payloadSize, durationSeconds, serverPort + mode);
            if (res != null) results.Add(res);
            await Task.Delay(1000);
         }

         RenderComparativeResultsTable(results);
      }
      else
      {
         var res = await RunBenchmarkForTransport(transportChoice, concurrentClients, sendsPerConnection, payloadSize, durationSeconds, serverPort);
         if (res != null)
         {
            RenderComparativeResultsTable([res]);
         }
      }

      Console.WriteLine("Benchmark completed.");
   }

   private static async Task<ChurnBenchmarkResult?> RunBenchmarkForTransport(
      int transportMode,
      int concurrentClients,
      int sendsPerConnection,
      int payloadSize,
      int durationSeconds,
      int serverPort)
   {
      var transportName = transportMode switch
      {
         1 => "TCP",
         2 => "WebSocket",
         3 => "QUIC",
         4 => "NamedPipes",
         5 => "UnixDomainSockets",
         6 => "Memory",
         7 => "UDP",
         _ => "TCP"
      };

      ConsoleRender.DrawHeader($"BENCHMARK: {transportName.ToUpper()}",
         $"Clients: {concurrentClients} | Sends/Conn: {sendsPerConnection} | Payload: {payloadSize}B | Duration: {durationSeconds}s");

      INetworkListener? listener = null;
      Func<INetworkClient>? clientFactory = null;
      X509Certificate2? cert = null;

      try
      {
         switch (transportMode)
         {
            case 1: // TCP
            {
               var endPoint = new IPEndPoint(IPAddress.Loopback, serverPort);
               var options = new TcpTransportOptions();
               listener = new TcpNetworkListener(endPoint, options);
               clientFactory = () => new TcpNetworkClient(options);
               break;
            }
            case 2: // WebSocket
            {
               var endPoint = new IPEndPoint(IPAddress.Loopback, serverPort);
               var options = new WsTransportOptions();
               listener = new WsNetworkListener(endPoint, options);
               clientFactory = () => new WsNetworkClient(options);
               break;
            }
            case 3: // QUIC
            {
               if (!QuicConnection.IsSupported || !QuicListener.IsSupported)
               {
                  Console.ForegroundColor = ConsoleColor.Yellow;
                  Console.WriteLine("QUIC is not supported on this platform/OS. Skipping QUIC benchmark.");
                  Console.ResetColor();
                  return null;
               }

               var endPoint = new IPEndPoint(IPAddress.Loopback, serverPort);
               cert = CertificateHelper.GenerateSelfSignedCertificate();
               var quicOptions = new QuicTransportOptions
               {
                  SslServerOptions = new SslServerAuthenticationOptions
                  {
                     ServerCertificate = cert
                  },
                  SslClientOptions = new SslClientAuthenticationOptions
                  {
                     TargetHost = "localhost",
                     RemoteCertificateValidationCallback = (sender, certificate, chain, errors) => true
                  }
               };
               listener = new QuicNetworkListener(endPoint, quicOptions);
               clientFactory = () => new QuicNetworkClient(quicOptions);
               break;
            }
            case 4: // Named Pipes
            {
               var pipeName = $"beskar-churn-{Guid.NewGuid():N}";
               var endPoint = new NamedPipeEndPoint(pipeName);
               var options = new NamedPipeTransportOptions();
               listener = new NamedPipeNetworkListener(endPoint, options);
               clientFactory = () => new NamedPipeNetworkClient(options);
               break;
            }
            case 5: // UDS
            {
               var socketPath = Path.Combine(Path.GetTempPath(), $"beskar_churn_{Guid.NewGuid():N}.sock");
               var endPoint = new UnixDomainSocketEndPoint(socketPath);
               var options = new UdsTransportOptions();
               listener = new UdsNetworkListener(endPoint, options);
               clientFactory = () => new UdsNetworkClient(options);
               break;
            }
            case 6: // Memory
            {
               var channelName = $"beskar-churn-mem-{Guid.NewGuid():N}";
               var endPoint = new MemoryEndPoint(channelName);
               var options = new MemoryTransportOptions();
               listener = new MemoryNetworkListener(endPoint, options);
               clientFactory = () => new MemoryNetworkClient(options);
               break;
            }
            case 7: // UDP
            {
               var endPoint = new IPEndPoint(IPAddress.Loopback, serverPort);
               var options = new UdpTransportOptions();
               listener = new UdpNetworkListener(endPoint, options);
               clientFactory = () => new UdpNetworkClient(options);
               break;
            }
         }

         if (listener == null || clientFactory == null) return null;

         var runner = new ChurnBenchmarkRunner(
            transportName,
            listener,
            clientFactory,
            concurrentClients,
            sendsPerConnection,
            payloadSize,
            durationSeconds
         );

         return await runner.RunAsync();
      }
      finally
      {
         cert?.Dispose();
      }
   }

   private static void RenderComparativeResultsTable(List<ChurnBenchmarkResult> results)
   {
      ConsoleRender.DrawHeader("FINAL COMPARATIVE BENCHMARK RESULTS",
         "Performance and memory metrics across lower-level transports");

      var table = ConsoleRender.CreateTable()
         .AddColumn("Transport", Alignment.Left, ConsoleColor.Cyan)
         .AddColumn("Connections", Alignment.Right, ConsoleColor.White)
         .AddColumn("Reconnects/s", Alignment.Right, ConsoleColor.Yellow)
         .AddColumn("Sent Msgs", Alignment.Right, ConsoleColor.White)
         .AddColumn("Throughput (MB/s)", Alignment.Right, ConsoleColor.Green)
         .AddColumn("Mem Growth (MB)", Alignment.Right, ConsoleColor.Magenta);

      foreach (var r in results)
      {
         table.AddRow(
            r.TransportName,
            r.ConnectionsCompleted.ToString("N0"),
            r.ReconnectRatePerSec.ToString("F1"),
            r.PacketsSent.ToString("N0"),
            r.ThroughputMBs.ToString("F2"),
            r.MemoryGrowthMB.ToString("F2")
         );
      }

      table.Render();
      Console.WriteLine();
   }

   private static int PromptInt(string prompt, int defaultValue)
   {
      Console.Write($"{prompt} [default: {defaultValue}]: ");
      var input = Console.ReadLine();
      if (string.IsNullOrWhiteSpace(input)) return defaultValue;

      if (int.TryParse(input, out var value)) return value;

      Console.ForegroundColor = ConsoleColor.Red;
      Console.WriteLine($"Invalid input, using default value: {defaultValue}");
      Console.ResetColor();
      return defaultValue;
   }
}

public record ChurnBenchmarkResult(
   string TransportName,
   long ConnectionsCompleted,
   double ReconnectRatePerSec,
   long PacketsSent,
   long PacketsReceived,
   double ThroughputMBs,
   double MemoryGrowthMB
);

public sealed class ChurnBenchmarkRunner
{
   private readonly string _transportName;
   private readonly INetworkListener _listener;
   private readonly Func<INetworkClient> _clientFactory;
   private readonly int _concurrentClients;
   private readonly int _sendsPerConnection;
   private readonly int _payloadSize;
   private readonly int _durationSeconds;

   private long _connectionsOpened;
   private long _connectionsClosed;
   private long _connectionFailures;
   private long _packetsSent;
   private long _packetsReceived;
   private long _bytesTransferred;

   public ChurnBenchmarkRunner(
      string transportName,
      INetworkListener listener,
      Func<INetworkClient> clientFactory,
      int concurrentClients,
      int sendsPerConnection,
      int payloadSize,
      int durationSeconds)
   {
      _transportName = transportName;
      _listener = listener;
      _clientFactory = clientFactory;
      _concurrentClients = concurrentClients;
      _sendsPerConnection = sendsPerConnection;
      _payloadSize = payloadSize;
      _durationSeconds = durationSeconds;
   }

   public async Task<ChurnBenchmarkResult> RunAsync()
   {
      var bindResult = await _listener.BindAsync();
      if (bindResult.Failed)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"Failed to bind {_transportName} listener: {bindResult.Error.Message}");
         Console.ResetColor();
         return new ChurnBenchmarkResult(_transportName, 0, 0, 0, 0, 0, 0);
      }

      using var cts = new CancellationTokenSource();
      var token = cts.Token;

      var payload = new byte[_payloadSize];
      RandomNumberGenerator.Fill(payload);

      // Server Accept Loop
      var serverAcceptTask = Task.Run(async () =>
      {
         var serverTasks = new List<Task>();
         while (!token.IsCancellationRequested)
         {
            try
            {
               var acceptResult = await _listener.AcceptSessionAsync(token);
               if (acceptResult.Failed) break;

               var session = acceptResult.Success!;
               serverTasks.Add(Task.Run(async () =>
               {
                  try
                  {
                     var streamResult = await session.AcceptStreamAsync(token);
                     if (!streamResult.Failed)
                     {
                        var stream = streamResult.Success!;
                        try
                        {
                           var input = stream.Transport.Input;
                           while (!token.IsCancellationRequested)
                           {
                              var readResult = await input.ReadAsync(token);
                              if (readResult.IsCompleted || readResult.IsCanceled) break;

                              var buffer = readResult.Buffer;
                              Interlocked.Add(ref _packetsReceived, 1);
                              input.AdvanceTo(buffer.End);
                           }
                        }
                        catch { }
                        finally
                        {
                           await stream.DisposeAsync();
                        }
                     }
                  }
                  catch { }
                  finally
                  {
                     await session.DisposeAsync();
                  }
               }, token));
            }
            catch { break; }
         }
         await Task.WhenAll(serverTasks);
      }, token);

      GC.Collect(2, GCCollectionMode.Forced, blocking: true);
      GC.WaitForPendingFinalizers();
      GC.Collect(2, GCCollectionMode.Forced, blocking: true);
      var initialMemory = Process.GetCurrentProcess().PrivateMemorySize64;

      var stopwatch = Stopwatch.StartNew();

      // Reporter Task
      var reporterTask = Task.Run(async () =>
      {
         long prevConnectionsClosed = 0;
         long prevSentBytes = 0;
         var reportSw = Stopwatch.StartNew();

         while (!token.IsCancellationRequested)
         {
            try
            {
               await Task.Delay(1000, token);
            }
            catch (OperationCanceledException) { break; }

            var elapsedSec = reportSw.Elapsed.TotalSeconds;
            reportSw.Restart();

            var currentClosed = Interlocked.Read(ref _connectionsClosed);
            var currentBytes = Interlocked.Read(ref _bytesTransferred);

            var diffClosed = currentClosed - prevConnectionsClosed;
            var diffBytes = currentBytes - prevSentBytes;

            prevConnectionsClosed = currentClosed;
            prevSentBytes = currentBytes;

            var reconnectRate = diffClosed / elapsedSec;
            var mbRate = diffBytes / elapsedSec / (1024 * 1024);

            var poolStats = SharedTransportMemoryPool.GetStats();
            var processMemMb = Process.GetCurrentProcess().PrivateMemorySize64 / (1024.0 * 1024.0);

            Console.WriteLine(
               $"[{stopwatch.Elapsed:hh\\:mm\\:ss}] Reconnects: {reconnectRate:F1}/s | Sent: {mbRate:F2} MB/s | Mem: {processMemMb:F1} MB | Pool [Rented: {poolStats.Rented:N0}, InStore: {poolStats.InStore:N0}, Created: {poolStats.Created:N0}]");
         }
      }, token);

      // Client Worker Tasks
      var clientWorkers = new List<Task>();
      for (var w = 0; w < _concurrentClients; w++)
      {
         clientWorkers.Add(Task.Run(async () =>
         {
            while (!token.IsCancellationRequested)
            {
               INetworkClient? client = null;
               INetworkSession? session = null;
               try
               {
                  Interlocked.Increment(ref _connectionsOpened);
                  client = _clientFactory();
                  var connectResult = await client.ConnectAsync(_listener.LocalAddress, token);
                  if (connectResult.Failed)
                  {
                     Interlocked.Increment(ref _connectionFailures);
                     await Task.Delay(50, token);
                     continue;
                  }

                  session = connectResult.Success!;
                  var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, token);
                  if (!streamResult.Failed)
                  {
                     var stream = streamResult.Success!;
                     var output = stream.Transport.Output;

                     for (var s = 0; s < _sendsPerConnection; s++)
                     {
                        if (token.IsCancellationRequested) break;
                        await output.WriteAsync(payload, token);
                        var flushResult = await output.FlushAsync(token);
                        if (flushResult.IsCompleted || flushResult.IsCanceled) break;

                        Interlocked.Increment(ref _packetsSent);
                        Interlocked.Add(ref _bytesTransferred, payload.Length);
                     }

                     await stream.DisposeAsync();
                  }

                  await client.DisconnectAsync();
                  Interlocked.Increment(ref _connectionsClosed);
               }
               catch (OperationCanceledException) { break; }
               catch
               {
                  Interlocked.Increment(ref _connectionFailures);
               }
               finally
               {
                  if (session != null) await session.DisposeAsync();
                  if (client != null) await client.DisposeAsync();
               }
            }
         }, token));
      }

      await Task.Delay(TimeSpan.FromSeconds(_durationSeconds));
      await cts.CancelAsync();

      try { await Task.WhenAll(clientWorkers); } catch { }
      try { await serverAcceptTask; } catch { }
      stopwatch.Stop();
      try { await reporterTask; } catch { }

      try
      {
         await _listener.UnbindAsync();
         await _listener.DisposeAsync();
      }
      catch { }

      GC.Collect(2, GCCollectionMode.Forced, blocking: true);
      GC.WaitForPendingFinalizers();
      GC.Collect(2, GCCollectionMode.Forced, blocking: true);

      var finalMemory = Process.GetCurrentProcess().PrivateMemorySize64;
      var memoryGrowthMB = (finalMemory - initialMemory) / (1024.0 * 1024.0);

      var totalDuration = stopwatch.Elapsed.TotalSeconds;
      var totalClosed = Interlocked.Read(ref _connectionsClosed);
      var totalSent = Interlocked.Read(ref _packetsSent);
      var totalRecv = Interlocked.Read(ref _packetsReceived);
      var totalBytes = Interlocked.Read(ref _bytesTransferred);

      var reconnectRateSec = totalClosed / totalDuration;
      var mbRateTotal = (totalBytes / totalDuration) / (1024 * 1024);

      return new ChurnBenchmarkResult(
         _transportName,
         totalClosed,
         reconnectRateSec,
         totalSent,
         totalRecv,
         mbRateTotal,
         memoryGrowthMB
      );
   }
}
