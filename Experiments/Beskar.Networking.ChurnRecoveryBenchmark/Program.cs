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

namespace Beskar.Networking.ChurnRecoveryBenchmark;

public static class Program
{
   public static async Task Main(string[] args)
   {
      TraceLogger.IsEnabled = false;

      ConsoleRender.DrawHeader("BESKAR CHURN & MEMORY RECOVERY BENCHMARK",
         "Measures native memory drain & pool cleanup after high connection churn stops");

      var transportChoice = 3; // Default to QUIC for quick testing
      var concurrentClients = 200;
      var sendsPerConnection = 50;
      var churnDurationSeconds = 10;
      var cooldownDurationSeconds = 20;
      var payloadSize = 512;
      var serverPort = 9300;

      if (args.Length >= 5)
      {
         int.TryParse(args[0], out transportChoice);
         int.TryParse(args[1], out concurrentClients);
         int.TryParse(args[2], out sendsPerConnection);
         int.TryParse(args[3], out churnDurationSeconds);
         int.TryParse(args[4], out cooldownDurationSeconds);
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

         transportChoice = PromptInt("Transport choice", 3);
         concurrentClients = PromptInt("Concurrent clients (parallel workers)", 200);
         sendsPerConnection = PromptInt("Sends per connection before graceful disconnect", 50);
         churnDurationSeconds = PromptInt("Phase 1: Churn duration (seconds)", 10);
         cooldownDurationSeconds = PromptInt("Phase 2: Cooldown & drain duration (seconds)", 20);
         payloadSize = PromptInt("Payload size per message (bytes)", 512);
         serverPort = PromptInt("Base server port", 9300);
      }

      Console.WriteLine();

      if (transportChoice == 8)
      {
         var results = new List<RecoveryBenchmarkResult>();
         for (var mode = 1; mode <= 7; mode++)
         {
            var res = await RunRecoveryTestForTransport(
               mode, concurrentClients, sendsPerConnection, churnDurationSeconds, cooldownDurationSeconds, payloadSize, serverPort + mode);
            if (res != null) results.Add(res);
            await Task.Delay(1000);
         }

         RenderComparativeRecoveryTable(results);
      }
      else
      {
         var res = await RunRecoveryTestForTransport(
            transportChoice, concurrentClients, sendsPerConnection, churnDurationSeconds, cooldownDurationSeconds, payloadSize, serverPort);
         if (res != null)
         {
            RenderComparativeRecoveryTable([res]);
         }
      }

      Console.WriteLine("Memory recovery benchmark completed.");
   }

   private static async Task<RecoveryBenchmarkResult?> RunRecoveryTestForTransport(
      int transportMode,
      int concurrentClients,
      int sendsPerConnection,
      int churnDurationSeconds,
      int cooldownDurationSeconds,
      int payloadSize,
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

      ConsoleRender.DrawHeader($"MEMORY RECOVERY TEST: {transportName.ToUpper()}",
         $"Workers: {concurrentClients} | Churn: {churnDurationSeconds}s | Cooldown (No new conns): {cooldownDurationSeconds}s");

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
                  IdleTimeout = TimeSpan.FromSeconds(5),
                  HandshakeTimeout = TimeSpan.FromSeconds(3),
                  MaxPendingConnections = 64,
                  MaxInboundBidirectionalStreams = 10,
                  MaxInboundUnidirectionalStreams = 10,
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
               var pipeName = $"beskar-rec-{Guid.NewGuid():N}";
               var endPoint = new NamedPipeEndPoint(pipeName);
               var options = new NamedPipeTransportOptions();
               listener = new NamedPipeNetworkListener(endPoint, options);
               clientFactory = () => new NamedPipeNetworkClient(options);
               break;
            }
            case 5: // UDS
            {
               var socketPath = Path.Combine(Path.GetTempPath(), $"beskar_rec_{Guid.NewGuid():N}.sock");
               var endPoint = new UnixDomainSocketEndPoint(socketPath);
               var options = new UdsTransportOptions();
               listener = new UdsNetworkListener(endPoint, options);
               clientFactory = () => new UdsNetworkClient(options);
               break;
            }
            case 6: // Memory
            {
               var channelName = $"beskar-rec-mem-{Guid.NewGuid():N}";
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

         var runner = new RecoveryBenchmarkRunner(
            transportName,
            listener,
            clientFactory,
            concurrentClients,
            sendsPerConnection,
            payloadSize,
            churnDurationSeconds,
            cooldownDurationSeconds
         );

         return await runner.RunAsync();
      }
      finally
      {
         cert?.Dispose();
      }
   }

   private static void RenderComparativeRecoveryTable(List<RecoveryBenchmarkResult> results)
   {
      ConsoleRender.DrawHeader("FINAL MEMORY DRAIN & RECOVERY SUMMARY",
         "Demonstrates user-space native memory freeing once connection churn stops");

      var table = ConsoleRender.CreateTable()
         .AddColumn("Transport", Alignment.Left, ConsoleColor.Cyan)
         .AddColumn("Baseline (MB)", Alignment.Right, ConsoleColor.White)
         .AddColumn("Peak Churn (MB)", Alignment.Right, ConsoleColor.Yellow)
         .AddColumn("Final Cooldown (MB)", Alignment.Right, ConsoleColor.Green)
         .AddColumn("Freed Memory (MB)", Alignment.Right, ConsoleColor.Magenta)
         .AddColumn("Recovery %", Alignment.Right, ConsoleColor.Cyan);

      foreach (var r in results)
      {
         table.AddRow(
            r.TransportName,
            r.BaselineMemoryMB.ToString("F1"),
            r.PeakMemoryMB.ToString("F1"),
            r.FinalMemoryMB.ToString("F1"),
            r.FreedMemoryMB.ToString("F1"),
            $"{r.RecoveryPercent:F1}%"
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

public record RecoveryBenchmarkResult(
   string TransportName,
   double BaselineMemoryMB,
   double PeakMemoryMB,
   double FinalMemoryMB,
   double FreedMemoryMB,
   double RecoveryPercent
);

public sealed class RecoveryBenchmarkRunner
{
   private readonly string _transportName;
   private readonly INetworkListener _listener;
   private readonly Func<INetworkClient> _clientFactory;
   private readonly int _concurrentClients;
   private readonly int _sendsPerConnection;
   private readonly int _payloadSize;
   private readonly int _churnDurationSeconds;
   private readonly int _cooldownDurationSeconds;

   private long _connectionsClosed;
   private long _packetsSent;

   public RecoveryBenchmarkRunner(
      string transportName,
      INetworkListener listener,
      Func<INetworkClient> clientFactory,
      int concurrentClients,
      int sendsPerConnection,
      int payloadSize,
      int churnDurationSeconds,
      int cooldownDurationSeconds)
   {
      _transportName = transportName;
      _listener = listener;
      _clientFactory = clientFactory;
      _concurrentClients = concurrentClients;
      _sendsPerConnection = sendsPerConnection;
      _payloadSize = payloadSize;
      _churnDurationSeconds = churnDurationSeconds;
      _cooldownDurationSeconds = cooldownDurationSeconds;
   }

   public async Task<RecoveryBenchmarkResult> RunAsync()
   {
      var bindResult = await _listener.BindAsync();
      if (bindResult.Failed)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"Failed to bind {_transportName} listener: {bindResult.Error.Message}");
         Console.ResetColor();
         return new RecoveryBenchmarkResult(_transportName, 0, 0, 0, 0, 0);
      }

      using var churnCts = new CancellationTokenSource();
      var churnToken = churnCts.Token;

      var payload = new byte[_payloadSize];
      RandomNumberGenerator.Fill(payload);

      // Server Accept Loop
      var serverAcceptTask = Task.Run(async () =>
      {
         var serverTasks = new List<Task>();
         while (!churnToken.IsCancellationRequested)
         {
            try
            {
               var acceptResult = await _listener.AcceptSessionAsync(churnToken);
               if (acceptResult.Failed) break;

               var session = acceptResult.Success!;
               serverTasks.Add(Task.Run(async () =>
               {
                  try
                  {
                     var streamResult = await session.AcceptStreamAsync(churnToken);
                     if (!streamResult.Failed)
                     {
                        var stream = streamResult.Success!;
                        try
                        {
                           var input = stream.Transport.Input;
                           while (!churnToken.IsCancellationRequested)
                           {
                              var readResult = await input.ReadAsync(churnToken);
                              if (readResult.IsCompleted || readResult.IsCanceled) break;
                              input.AdvanceTo(readResult.Buffer.End);
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
               }, churnToken));
            }
            catch { break; }
         }
         await Task.WhenAll(serverTasks);
      });

      GC.Collect(2, GCCollectionMode.Forced, blocking: true);
      GC.WaitForPendingFinalizers();
      GC.Collect(2, GCCollectionMode.Forced, blocking: true);
      var baselineMemMb = Process.GetCurrentProcess().PrivateMemorySize64 / (1024.0 * 1024.0);

      Console.ForegroundColor = ConsoleColor.Yellow;
      Console.WriteLine($"=== PHASE 1: HIGH CONNECTION CHURN ({_churnDurationSeconds} SECONDS) ===");
      Console.ResetColor();

      // Client Worker Tasks
      var clientWorkers = new List<Task>();
      for (var w = 0; w < _concurrentClients; w++)
      {
         clientWorkers.Add(Task.Run(async () =>
         {
            var client = _clientFactory();
            try
            {
               while (!churnToken.IsCancellationRequested)
               {
                  INetworkSession? session = null;
                  try
                  {
                     var connectResult = await client.ConnectAsync(_listener.LocalAddress, churnToken);
                     if (connectResult.Failed)
                     {
                        await Task.Delay(50, churnToken);
                        continue;
                     }

                     session = connectResult.Success!;
                     var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, churnToken);
                     if (!streamResult.Failed)
                     {
                        var stream = streamResult.Success!;
                        var output = stream.Transport.Output;

                        for (var s = 0; s < _sendsPerConnection; s++)
                        {
                           if (churnToken.IsCancellationRequested) break;
                           await output.WriteAsync(payload, churnToken);
                           var flushResult = await output.FlushAsync(churnToken);
                           if (flushResult.IsCompleted || flushResult.IsCanceled) break;
                           Interlocked.Increment(ref _packetsSent);
                        }

                        await stream.DisposeAsync();
                     }

                     await client.DisconnectAsync();
                     Interlocked.Increment(ref _connectionsClosed);
                  }
                  catch (OperationCanceledException) { break; }
                  catch { }
                  finally
                  {
                     if (session != null) await session.DisposeAsync();
                  }
               }
            }
            finally
            {
               await client.DisposeAsync();
            }
         }, churnToken));
      }

      // Track Phase 1 Churn Memory
      for (var sec = 1; sec <= _churnDurationSeconds; sec++)
      {
         await Task.Delay(1000);
         var currentMemMb = Process.GetCurrentProcess().PrivateMemorySize64 / (1024.0 * 1024.0);
         var closed = Interlocked.Read(ref _connectionsClosed);
         Console.WriteLine($"[CHURN {sec:D2}s/{_churnDurationSeconds:D2}s] Active Churn! Conns: {closed:N0} | Process Memory: {currentMemMb:F1} MB");
      }

      // STOP CHURN WORKERS
      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine();
      Console.WriteLine(">>> STOPPING CLIENT CONNECT WORKERS - ZERO NEW CONNECTIONS CREATED <<<");
      Console.ResetColor();

      await churnCts.CancelAsync();
      try { await Task.WhenAll(clientWorkers); } catch { }
      try { await serverAcceptTask; } catch { }

      try
      {
         await _listener.UnbindAsync();
         await _listener.DisposeAsync();
      }
      catch { }

      var peakMemMb = Process.GetCurrentProcess().PrivateMemorySize64 / (1024.0 * 1024.0);

      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine();
      Console.WriteLine($"=== PHASE 2: COOLDOWN & NATIVE MEMORY DRAIN ({_cooldownDurationSeconds} SECONDS) ===");
      Console.WriteLine($"Peak Memory at End of Churn: {peakMemMb:F1} MB");
      Console.ResetColor();

      // Monitor Memory Recovery During Cooldown
      for (var sec = 1; sec <= _cooldownDurationSeconds; sec++)
      {
         await Task.Delay(1000);

         if (sec % 5 == 0)
         {
            GC.Collect(2, GCCollectionMode.Forced, blocking: true);
            GC.WaitForPendingFinalizers();
            GC.Collect(2, GCCollectionMode.Forced, blocking: true);
         }

         var currentMemMb = Process.GetCurrentProcess().PrivateMemorySize64 / (1024.0 * 1024.0);
         var freedSoFarMb = peakMemMb - currentMemMb;
         Console.WriteLine($"[COOLDOWN {sec:D2}s/{_cooldownDurationSeconds:D2}s] Idle State | Process Memory: {currentMemMb:F1} MB | Freed: {freedSoFarMb:F1} MB");
      }

      Console.ForegroundColor = ConsoleColor.Yellow;
      Console.WriteLine();
      Console.WriteLine($"=== PHASE 3: SECOND CONNECTION CHURN ({_churnDurationSeconds} SECONDS) - TESTING HEAP REUSE ===");
      Console.ResetColor();

      // Re-bind listener for Phase 3
      var bindRes2 = await _listener.BindAsync();
      if (!bindRes2.Failed)
      {
         using var churnCts2 = new CancellationTokenSource();
         var churnToken2 = churnCts2.Token;

         var serverAcceptTask2 = Task.Run(async () =>
         {
            var serverTasks = new List<Task>();
            while (!churnToken2.IsCancellationRequested)
            {
               try
               {
                  var acceptResult = await _listener.AcceptSessionAsync(churnToken2);
                  if (acceptResult.Failed) break;

                  var session = acceptResult.Success!;
                  serverTasks.Add(Task.Run(async () =>
                  {
                     try
                     {
                        var streamResult = await session.AcceptStreamAsync(churnToken2);
                        if (!streamResult.Failed)
                        {
                           var stream = streamResult.Success!;
                           try
                           {
                              var input = stream.Transport.Input;
                              while (!churnToken2.IsCancellationRequested)
                              {
                                 var readResult = await input.ReadAsync(churnToken2);
                                 if (readResult.IsCompleted || readResult.IsCanceled) break;
                                 input.AdvanceTo(readResult.Buffer.End);
                              }
                           }
                           catch { }
                           finally { await stream.DisposeAsync(); }
                        }
                     }
                     catch { }
                     finally { await session.DisposeAsync(); }
                  }, churnToken2));
               }
               catch { break; }
            }
            await Task.WhenAll(serverTasks);
         });

         var clientWorkers2 = new List<Task>();
         for (var w = 0; w < _concurrentClients; w++)
         {
            clientWorkers2.Add(Task.Run(async () =>
            {
               var client = _clientFactory();
               try
               {
                  while (!churnToken2.IsCancellationRequested)
                  {
                     INetworkSession? session = null;
                     try
                     {
                        var connectResult = await client.ConnectAsync(_listener.LocalAddress, churnToken2);
                        if (connectResult.Failed)
                        {
                           await Task.Delay(50, churnToken2);
                           continue;
                        }

                        session = connectResult.Success!;
                        var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, churnToken2);
                        if (!streamResult.Failed)
                        {
                           var stream = streamResult.Success!;
                           var output = stream.Transport.Output;
                           for (var s = 0; s < _sendsPerConnection; s++)
                           {
                              if (churnToken2.IsCancellationRequested) break;
                              await output.WriteAsync(payload, churnToken2);
                              var flushResult = await output.FlushAsync(churnToken2);
                              if (flushResult.IsCompleted || flushResult.IsCanceled) break;
                           }
                           await stream.DisposeAsync();
                        }
                        await client.DisconnectAsync();
                        Interlocked.Increment(ref _connectionsClosed);
                     }
                     catch (OperationCanceledException) { break; }
                     catch { }
                     finally
                     {
                        if (session != null) await session.DisposeAsync();
                     }
                  }
               }
               finally
               {
                  await client.DisposeAsync();
               }
            }, churnToken2));
         }

         for (var sec = 1; sec <= _churnDurationSeconds; sec++)
         {
            await Task.Delay(1000);
            var currentMemMb = Process.GetCurrentProcess().PrivateMemorySize64 / (1024.0 * 1024.0);
            var closed = Interlocked.Read(ref _connectionsClosed);
            Console.WriteLine($"[CHURN 2 {sec:D2}s/{_churnDurationSeconds:D2}s] Phase 3 Churn! Conns: {closed:N0} | Process Memory: {currentMemMb:F1} MB");
         }

         await churnCts2.CancelAsync();
         try { await Task.WhenAll(clientWorkers2); } catch { }
         try { await serverAcceptTask2; } catch { }

         try
         {
            await _listener.UnbindAsync();
            await _listener.DisposeAsync();
         }
         catch { }
      }

      // Final GC collection
      GC.Collect(2, GCCollectionMode.Forced, blocking: true);
      GC.WaitForPendingFinalizers();
      GC.Collect(2, GCCollectionMode.Forced, blocking: true);

      var finalMemMb = Process.GetCurrentProcess().PrivateMemorySize64 / (1024.0 * 1024.0);
      var totalFreedMb = Math.Max(0, peakMemMb - finalMemMb);
      var memoryAddedInChurn = Math.Max(0.1, peakMemMb - baselineMemMb);
      var recoveryPercent = Math.Min(100.0, (totalFreedMb / memoryAddedInChurn) * 100.0);

      try
      {
         await _listener.UnbindAsync();
         await _listener.DisposeAsync();
      }
      catch { }

      return new RecoveryBenchmarkResult(
         _transportName,
         baselineMemMb,
         peakMemMb,
         finalMemMb,
         totalFreedMb,
         recoveryPercent
      );
   }
}
