using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Telemetry;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Udp;
using Beskar.Networking.Transports.Ws;
using Beskar.Networking.Transports.Uds;
using Beskar.Networking.Transports.NamedPipes;
using Beskar.Networking.Transports.Memory;
using Beskar.Networking.Transports.Quic;
using Beskar.Utilities.Console.Rendering;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Transports.ChaosSimulator;

public static class Program
{
   private static readonly ConcurrentDictionary<string, long> TelemetryGauges = new();
   private static readonly ConcurrentDictionary<string, long> TelemetryCounters = new();
   private static readonly MeterListener MeterListener = new();

   // Statistics counters
   private static long _clientConnectionAttempts;
   private static long _clientConnectionFailures;
   private static long _clientConnectionsEstablished;
   private static long _clientConnectionsLost;

   private static long _packetsSent;
   private static long _packetsReceived;
   private static long _checksumFailures;
   private static long _droppedPackets;
   private static long _outOfOrderPackets;

   private static long _totalLatencyMs;
   private static long _latencyCount;
   private static long _minLatencyMs = 999999;
   private static long _maxLatencyMs;

   private static long _bytesTransferred;

   private static int _activeServerSessions;
   private static int _activeClientConnections;
   private static int _concurrentClients;

   private static readonly Lock LogLock = new();

   public static async Task Main(string[] args)
   {
      // Disable noisy internal trace logging to clean up output
      TraceLogger.IsEnabled = false;

      MeterListener.InstrumentPublished = (instrument, listener) =>
      {
         if (instrument.Meter.Name == TransportMetrics.MeterName)
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };

      MeterListener.SetMeasurementEventCallback<long>((instrument, measurement, tags, state) =>
      {
         var name = instrument.Name;
         if (instrument is UpDownCounter<long>)
         {
            TelemetryGauges.AddOrUpdate(name, measurement, (_, prev) => prev + measurement);
         }
         else if (instrument is Counter<long>)
         {
            TelemetryCounters.AddOrUpdate(name, measurement, (_, prev) => prev + measurement);
         }
      });

      MeterListener.SetMeasurementEventCallback<double>((instrument, measurement, tags, state) =>
      {
         var name = instrument.Name;
         TelemetryGauges.AddOrUpdate(name, (long)measurement, (_, _) => (long)measurement);
      });

      MeterListener.Start();

      SafeClear();

      ConsoleRender.DrawHeader("BESKAR TRANSPORT CHAOS SIMULATOR",
         "Hostile testing framework for lower-level transport abstraction interfaces");

      var transportOption = 1;
      var chaosOption = 6; // Default to Total Chaos in automated runs
      var concurrentClients = 1;

      if (args.Length >= 2 && int.TryParse(args[0], out var tArg) && int.TryParse(args[1], out var cArg))
      {
         transportOption = tArg;
         chaosOption = cArg;
         if (args.Length >= 3 && int.TryParse(args[2], out var ccArg))
         {
            concurrentClients = ccArg;
         }
      }
      else if (Console.IsInputRedirected)
      {
         transportOption = 1; // TCP
         chaosOption = 6;     // Total Chaos
         concurrentClients = 3; // Default 3 in automated runs
      }
      else
      {
         ConsoleRender.Info("Choose the underlying transport to test:");
         Console.WriteLine("  1. Transmission Control Protocol (TCP)");
         Console.WriteLine("  2. WebSocket (WS)");
         Console.WriteLine("  3. User Datagram Protocol (UDP)");
         Console.WriteLine("  4. Quick UDP Internet Connections (QUIC)");
         Console.WriteLine("  5. Unix Domain Sockets (UDS)");
         Console.WriteLine("  6. Named Pipes");
         Console.WriteLine("  7. In-Memory Transport");
         Console.Write("\nSelect Option (1-7): ");
         var transportInput = Console.ReadLine();
         if (int.TryParse(transportInput, out var opt) && opt >= 1 && opt <= 7)
         {
            transportOption = opt;
         }

         if (transportOption == 4 && !QuicConnection.IsSupported)
         {
            ConsoleRender.Error("QUIC is not supported on this platform/OS. Falling back to TCP.");
            transportOption = 1;
            await Task.Delay(1500);
         }

         SafeClear();
         ConsoleRender.DrawHeader("BESKAR TRANSPORT CHAOS SIMULATOR", "Choose Chaos Profile");
         Console.WriteLine("  1. Clean Link (No injected failures/baseline)");
         Console.WriteLine("  2. Flaky Link (High packet drops, session disconnects, connect failures)");
         Console.WriteLine("  3. High Latency Link (Adds random delays of 100-300ms on all reads and writes)");
         Console.WriteLine("  4. Corrupted Link (Adds random 3% payload bit-flip corruption)");
         Console.WriteLine("  5. Throttled Pipe (Enforces 50 KB/s bandwidth limits and minor latency)");
         Console.WriteLine("  6. TOTAL CHAOS (Runs all profiles concurrently - disconnects, drops, latency, throttling, corruption)");
         Console.WriteLine("  7. Stream & Connection Churn (High connection & stream creation/abrupt disconnects for memory leak testing)");
         Console.Write("\nSelect Option (1-7): ");
         var chaosInput = Console.ReadLine();
         if (int.TryParse(chaosInput, out var cOpt) && cOpt >= 1 && cOpt <= 7)
         {
            chaosOption = cOpt;
         }

         SafeClear();
         ConsoleRender.DrawHeader("BESKAR TRANSPORT CHAOS SIMULATOR", "Choose Concurrency");
         Console.Write("Enter number of concurrent clients to run (default 1): ");
         var ccInput = Console.ReadLine();
         if (int.TryParse(ccInput, out var ccVal) && ccVal > 0)
         {
            concurrentClients = ccVal;
         }
      }

      _concurrentClients = concurrentClients;

      var options = chaosOption switch
      {
         1 => ChaosOptions.Clean,
         2 => ChaosOptions.Flaky,
         3 => ChaosOptions.Latent,
         4 => ChaosOptions.Corrupt,
         5 => ChaosOptions.Throttled,
         6 => ChaosOptions.TotalChaos,
         7 => ChaosOptions.ChurnAndLeak,
         _ => ChaosOptions.Clean
      };

      SafeClear();
      ConsoleRender.Success($"Starting Chaos Simulator with Profile: {options.ProfileName}");

      // Setup Address based on Transport selection
      EndPoint listenerAddress;
      string? tempUdsFile = null;

      switch (transportOption)
      {
         case 5: // UDS
            tempUdsFile = Path.Combine(Path.GetTempPath(), $"uds-chaos-{Guid.NewGuid():N}.sock");
            listenerAddress = new UnixDomainSocketEndPoint(tempUdsFile);
            break;
         case 6: // Named Pipes
            listenerAddress = new NamedPipeEndPoint($"chaos-pipe-{Guid.NewGuid():N}", ".");
            break;
         case 7: // Memory
            listenerAddress = new MemoryEndPoint($"chaos-mem-{Guid.NewGuid():N}");
            break;
         default: // TCP, WS, UDP, QUIC
            listenerAddress = new IPEndPoint(IPAddress.Loopback, 0);
            break;
      }

      // Create raw inner listener
      INetworkListener rawListener;

      X509Certificate2? quicCert = null;

      try
      {
         switch (transportOption)
         {
            case 1: // TCP
               var tcpOptions = new TcpTransportOptions();
               rawListener = new TcpNetworkListener(listenerAddress, tcpOptions);
               break;

            case 2: // WS
               var wsOptions = new WsTransportOptions();
               rawListener = new WsNetworkListener(listenerAddress, wsOptions);
               break;

            case 3: // UDP
               var udpOptions = new UdpTransportOptions();
               rawListener = new UdpNetworkListener(listenerAddress, udpOptions);
               break;

            case 4: // QUIC
               quicCert = CertificateUtility.GenerateSelfSignedCertificate();
               var quicOptions = new QuicTransportOptions
               {
                  AlpnProtocol = "chaos-quic",
                  SslServerOptions = new System.Net.Security.SslServerAuthenticationOptions
                  {
                     ServerCertificate = quicCert,
                     ApplicationProtocols = [new SslApplicationProtocol("chaos-quic")]
                  },
                  SslClientOptions = new System.Net.Security.SslClientAuthenticationOptions
                  {
                     ApplicationProtocols = [new SslApplicationProtocol("chaos-quic")],
                     RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
                  }
               };
               rawListener = new QuicNetworkListener(listenerAddress, quicOptions);
               break;

            case 5: // UDS
               var udsOptions = new UdsTransportOptions();
               rawListener = new UdsNetworkListener(listenerAddress, udsOptions);
               break;

            case 6: // Named Pipes
               var npOptions = new NamedPipeTransportOptions();
               rawListener = new NamedPipeNetworkListener(listenerAddress, npOptions);
               break;

            case 7: // Memory
               var memOptions = new MemoryTransportOptions();
               rawListener = new MemoryNetworkListener((MemoryEndPoint)listenerAddress, memOptions);
               break;

            default:
               throw new InvalidOperationException("Invalid transport selection.");
         }
      }
      catch (Exception ex)
      {
         ConsoleRender.Error($"Failed to initialize transport backend: {ex.Message}");
         quicCert?.Dispose();
         if (tempUdsFile is not null && File.Exists(tempUdsFile)) File.Delete(tempUdsFile);
         return;
      }

      // Wrap listener in our Chaos Decorator
      var listener = new ChaosNetworkListener(rawListener, options);

      using var cts = new CancellationTokenSource();

      // Bind listener
      var bindResult = await listener.BindAsync(cts.Token);
      if (bindResult.Failed)
      {
         ConsoleRender.Error($"Listener BindAsync failed: {bindResult.Error.Message}");
         quicCert?.Dispose();
         if (tempUdsFile is not null && File.Exists(tempUdsFile)) File.Delete(tempUdsFile);
         return;
      }

      ConsoleRender.Success($"Listener bound successfully. Actual Endpoint: {listener.LocalAddress}");

      // Start accepted sessions loop
      var serverTask = Task.Run(() => ServerAcceptLoopAsync(listener, cts.Token));

      // Start active client scenario loops
      var clientTasks = new List<Task>();
      for (var i = 0; i < concurrentClients; i++)
      {
         var clientIndex = i;
         clientTasks.Add(Task.Run(() => ClientScenarioLoopAsync(clientIndex, transportOption, options, listener.LocalAddress, cts.Token)));
      }

      // Start statistics dashboard loop
      var statsTask = Task.Run(() => DisplayDashboardLoopAsync(options, concurrentClients, cts.Token));

      if (Console.IsInputRedirected)
      {
         ConsoleRender.Info("Simulator is running in non-interactive mode. Running for 12 seconds...");
         try
         {
            await Task.Delay(12000, cts.Token);
         }
         catch (TaskCanceledException) { }
      }
      else
      {
         ConsoleRender.Warning("\nSimulator is now running. Press ENTER to stop the simulation...");
         Console.ReadLine();
      }

      ConsoleRender.Info("Shutting down simulation and cleanup...");
      await cts.CancelAsync();

      try
      {
         await Task.WhenAll(serverTask, statsTask);
         await Task.WhenAll(clientTasks);
      }
      catch
      {
         // Ignored
      }

      // Cleanup
      await listener.DisposeAsync();
      quicCert?.Dispose();

      if (tempUdsFile is not null && File.Exists(tempUdsFile))
      {
         try { File.Delete(tempUdsFile); } catch { /* Ignored */ }
      }

      SafeClear();
      ConsoleRender.Success("Chaos Simulator completed and cleaned up successfully.");
   }

   private static async Task ServerAcceptLoopAsync(INetworkListener listener, CancellationToken ct)
   {
      while (!ct.IsCancellationRequested)
      {
         try
         {
            var acceptResult = await listener.AcceptSessionAsync(ct);
            if (acceptResult.Failed)
            {
               continue;
            }

            var session = acceptResult.Success;
            _ = Task.Run(() => HandleServerSessionAsync(session, ct), ct);
         }
         catch (Exception)
         {
            // Accept loop error, ignore or continue
         }
      }
   }

    private static async Task HandleServerSessionAsync(INetworkSession session, CancellationToken ct)
    {
       Interlocked.Increment(ref _activeServerSessions);
       try
       {
          if (session.IsSupportingMultiplexing)
          {
             while (!ct.IsCancellationRequested && !session.SessionClosedToken.IsCancellationRequested)
             {
                var streamResult = await session.AcceptStreamAsync(ct);
                if (streamResult.Failed)
                {
                   break;
                }

                var stream = streamResult.Success;
                _ = Task.Run(() => HandleServerStreamAsync(stream, ct), ct);
             }
          }
          else
          {
             var streamResult = await session.AcceptStreamAsync(ct);
             if (!streamResult.Failed)
             {
                await HandleServerStreamAsync(streamResult.Success, ct);
             }
          }
       }
       catch
       {
          // Ignored
       }
       finally
       {
          Interlocked.Decrement(ref _activeServerSessions);
          await session.DisposeAsync();
       }
    }

   private static async Task HandleServerStreamAsync(INetworkStream stream, CancellationToken ct)
   {
      var expectedSequence = 0;
      try
      {
         while (!ct.IsCancellationRequested)
         {
            var packet = await ChaosPacket.ReadAsync(stream.Transport.Input, ct);
            if (packet is null)
            {
               break;
            }

            Interlocked.Increment(ref _packetsReceived);
            Interlocked.Add(ref _bytesTransferred, packet.Payload.Length);

            if (packet.IsCorrupted)
            {
               Interlocked.Increment(ref _checksumFailures);
               LogChaosEvent("SERVER", "CORRUPT", $"Payload checksum verification failed! Seq: {packet.SequenceNumber}", ConsoleColor.Red);
            }
            else
            {
               // Measure latency
               var latencyMs = (DateTimeOffset.UtcNow.Ticks - packet.Timestamp) / TimeSpan.TicksPerMillisecond;
               latencyMs = Math.Max(0, latencyMs);

               Interlocked.Add(ref _totalLatencyMs, latencyMs);
               Interlocked.Increment(ref _latencyCount);

               // Atomic update min/max
               long currentMin;
               while (latencyMs < (currentMin = Volatile.Read(ref _minLatencyMs)))
               {
                  Interlocked.CompareExchange(ref _minLatencyMs, latencyMs, currentMin);
               }

               long currentMax;
               while (latencyMs > (currentMax = Volatile.Read(ref _maxLatencyMs)))
               {
                  Interlocked.CompareExchange(ref _maxLatencyMs, latencyMs, currentMax);
               }

               // Verify sequence number
               if (packet.SequenceNumber != expectedSequence)
               {
                  if (packet.SequenceNumber > expectedSequence)
                  {
                     var gaps = packet.SequenceNumber - expectedSequence;
                     Interlocked.Add(ref _droppedPackets, gaps);
                     LogChaosEvent("SERVER", "DROP", $"Detected Sequence Gap. Expected: {expectedSequence}, Got: {packet.SequenceNumber}. Missing: {gaps}", ConsoleColor.Yellow);
                  }
                  else
                  {
                     Interlocked.Increment(ref _outOfOrderPackets);
                     LogChaosEvent("SERVER", "REORDER", $"Out-of-order/Duplicate Packet. Expected: {expectedSequence}, Got: {packet.SequenceNumber}", ConsoleColor.DarkYellow);
                  }
               }

               expectedSequence = packet.SequenceNumber + 1;
            }
         }
      }
      catch (Exception ex)
      {
         LogChaosEvent("SERVER", "ABORT", $"Session stream terminated abruptly: {ex.Message}", ConsoleColor.DarkRed);
      }
      finally
      {
         await stream.DisposeAsync();
      }
   }

   private static async Task ClientScenarioLoopAsync(int clientIndex, int transportOption, ChaosOptions chaosOpts, EndPoint endPoint, CancellationToken ct)
   {
      var currentSeq = 0;
      var payload = new byte[512];
      Random.Shared.NextBytes(payload);

      while (!ct.IsCancellationRequested)
      {
         Interlocked.Increment(ref _clientConnectionAttempts);
         var client = CreateClient(transportOption, chaosOpts);

         try
         {
            var connectResult = await client.ConnectAsync(endPoint, ct);
            if (connectResult.Failed)
            {
               Interlocked.Increment(ref _clientConnectionFailures);
               LogChaosEvent($"CLIENT-{clientIndex}", "CONN_FAIL", $"Failed to connect: {connectResult.Error.Message}", ConsoleColor.DarkGray);
               await client.DisposeAsync();
               await Task.Delay(1000, ct);
               continue;
            }

            Interlocked.Increment(ref _clientConnectionsEstablished);
            Interlocked.Increment(ref _activeClientConnections);
            LogChaosEvent($"CLIENT-{clientIndex}", "CONN_OK", "Established connection successfully.", ConsoleColor.Green);

            var session = connectResult.Success;
            try
            {
               var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
               if (streamResult.Failed)
               {
                  LogChaosEvent($"CLIENT-{clientIndex}", "STRM_FAIL", $"Failed to open stream: {streamResult.Error.Message}", ConsoleColor.Red);
                  throw new InvalidOperationException();
               }

               var stream = streamResult.Success;
               using var readCts = CancellationTokenSource.CreateLinkedTokenSource(ct, session.SessionClosedToken);
               _ = Task.Run(async () =>
               {
                  try
                  {
                     while (!readCts.Token.IsCancellationRequested)
                     {
                        var readResult = await stream.Transport.Input.ReadAsync(readCts.Token);
                        stream.Transport.Input.AdvanceTo(readResult.Buffer.End);
                        if (readResult.IsCompleted)
                        {
                           break;
                        }
                     }
                  }
                  catch
                  {
                     // Ignored
                  }
               }, readCts.Token);

               await using (stream)
               {
                  if (session.IsSupportingMultiplexing && chaosOpts == ChaosOptions.ChurnAndLeak)
                  {
                     // Open extra multiplexed stream inline to stress test stream creation & disposal without thread pool exhaustion
                     try
                     {
                        using var streamCts = CancellationTokenSource.CreateLinkedTokenSource(ct, session.SessionClosedToken);
                        var stResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, streamCts.Token);
                        if (!stResult.Failed)
                        {
                           await using var subStream = stResult.Success;
                           await ChaosPacket.WriteAsync(subStream.Transport.Output, currentSeq, payload, streamCts.Token);
                           Interlocked.Increment(ref _packetsSent);
                           currentSeq++;
                        }
                     }
                     catch
                     {
                        // Ignored
                     }
                  }

                  while (!ct.IsCancellationRequested && !session.SessionClosedToken.IsCancellationRequested)
                  {
                     // Send framed packet
                     await ChaosPacket.WriteAsync(stream.Transport.Output, currentSeq, payload, ct);
                     Interlocked.Increment(ref _packetsSent);

                     currentSeq++;

                     // Random delay between sends
                     await Task.Delay(Random.Shared.Next(50, 150), ct);
                  }
               }
            }
            catch (Exception ex)
            {
               Interlocked.Increment(ref _clientConnectionsLost);
               LogChaosEvent($"CLIENT-{clientIndex}", "CONN_LOST", $"Session dropped: {ex.Message}", ConsoleColor.Red);
            }
            finally
            {
               Interlocked.Decrement(ref _activeClientConnections);
               await session.DisposeAsync();
            }
         }
         catch (Exception ex)
         {
            LogChaosEvent($"CLIENT-{clientIndex}", "CONN_ERR", $"Connection error: {ex.Message}", ConsoleColor.Red);
         }
         finally
         {
            await client.DisposeAsync();
         }

         // Wait briefly before reconnecting
         var delayMs = chaosOpts == ChaosOptions.ChurnAndLeak ? Random.Shared.Next(20, 100) : Random.Shared.Next(1000, 2000);
         await Task.Delay(delayMs, ct);
      }
   }

   private static INetworkClient CreateClient(int transportOption, ChaosOptions chaosOpts)
   {
      INetworkClient rawClient = transportOption switch
      {
         1 => new TcpNetworkClient(new TcpTransportOptions()),
         2 => new WsNetworkClient(new WsTransportOptions()),
         3 => new UdpNetworkClient(new UdpTransportOptions()),
         4 => new QuicNetworkClient(new QuicTransportOptions
         {
            AlpnProtocol = "chaos-quic",
            SslClientOptions = new System.Net.Security.SslClientAuthenticationOptions
            {
               ApplicationProtocols = [new System.Net.Security.SslApplicationProtocol("chaos-quic")],
               RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
            }
         }),
         5 => new UdsNetworkClient(new UdsTransportOptions()),
         6 => new NamedPipeNetworkClient(new NamedPipeTransportOptions()),
         7 => new MemoryNetworkClient(new MemoryTransportOptions()),
         _ => throw new InvalidOperationException("Invalid transport selection.")
      };
      return new ChaosNetworkClient(rawClient, chaosOpts);
   }

   private static async Task DisplayDashboardLoopAsync(ChaosOptions options, int totalClients, CancellationToken ct)
   {
      while (!ct.IsCancellationRequested)
      {
         await Task.Delay(2000, ct);

         var elapsedMs = _latencyCount == 0 ? 0 : Volatile.Read(ref _totalLatencyMs);
         var count = Volatile.Read(ref _latencyCount);
         var avgLatency = count == 0 ? 0 : (double)elapsedMs / count;
         var minLat = count == 0 ? 0 : Volatile.Read(ref _minLatencyMs);
         var maxLat = count == 0 ? 0 : Volatile.Read(ref _maxLatencyMs);

         var speedKB = (double)Interlocked.Read(ref _bytesTransferred) / 1024.0 / 2.0; // divided by 2 seconds interval
         Interlocked.Exchange(ref _bytesTransferred, 0);

         lock (LogLock)
         {
            Console.WriteLine("\n[--- CHAOS SIMULATOR LIVE DASHBOARD ---]");
            Console.WriteLine($"Profile:            {options.ProfileName}");
            Console.WriteLine($"Client State:       {Volatile.Read(ref _activeClientConnections)} / {totalClients} Connected");
            Console.WriteLine($"Active Server Sess: {Volatile.Read(ref _activeServerSessions)}");
            Console.WriteLine($"Connection Stats:   Attempts={Volatile.Read(ref _clientConnectionAttempts)} Failures={Volatile.Read(ref _clientConnectionFailures)} Established={Volatile.Read(ref _clientConnectionsEstablished)} Lost={Volatile.Read(ref _clientConnectionsLost)}");
            Console.WriteLine($"Data Sent/Recv:     Sent={Volatile.Read(ref _packetsSent)} Recv={Volatile.Read(ref _packetsReceived)} Speed={speedKB:F1} KB/s");
            var poolStats = Beskar.Networking.Transports.Common.Options.SharedTransportMemoryPool.GetStats();
            var proc = System.Diagnostics.Process.GetCurrentProcess();
            var workingSetMB = proc.WorkingSet64 / (1024.0 * 1024.0);
            var privateBytesMB = proc.PrivateMemorySize64 / (1024.0 * 1024.0);
            var gcHeapMB = GC.GetTotalMemory(false) / (1024.0 * 1024.0);
            Console.WriteLine($"Memory Pools:       Rented={poolStats.Rented} Cached/InStore={poolStats.InStore} Created={poolStats.Created}");
            Console.WriteLine($"Process Memory:     WorkingSet={workingSetMB:F1} MB | PrivateBytes={privateBytesMB:F1} MB | GC Heap={gcHeapMB:F1} MB");
            Console.WriteLine($"Fault Metrics:      ChecksumFailures={Volatile.Read(ref _checksumFailures)} Gaps(Drops)={Volatile.Read(ref _droppedPackets)} OutOfOrder={Volatile.Read(ref _outOfOrderPackets)}");
            Console.WriteLine($"Latency (ms):       Min={minLat} Max={maxLat} Avg={avgLatency:F1}");
            Console.WriteLine("[---------------------------------------]");

            // Render Live System.Diagnostics.Metrics OpenTelemetry table
            var transportActiveConns = TelemetryGauges.GetValueOrDefault("beskar.transport.connections.active", 0);
            var transportOpenedConns = TelemetryCounters.GetValueOrDefault("beskar.transport.connections.opened", 0);
            var transportClosedConns = TelemetryCounters.GetValueOrDefault("beskar.transport.connections.closed", 0);
            var transportBytesSent = TelemetryCounters.GetValueOrDefault("beskar.transport.bytes.sent", 0);
            var transportBytesRecv = TelemetryCounters.GetValueOrDefault("beskar.transport.bytes.received", 0);

            ConsoleRender.CreateTable()
               .SetBorderColor(ConsoleColor.Magenta)
               .AddColumn("OpenTelemetry Meter", Alignment.Left, ConsoleColor.Magenta)
               .AddColumn("Instrument Name", Alignment.Left, ConsoleColor.Yellow)
               .AddColumn("Type / Unit", Alignment.Left, ConsoleColor.Cyan)
               .AddColumn("Live Value", Alignment.Right, ConsoleColor.White)
               .AddRow("Beskar.Networking.Transport", "beskar.transport.connections.active", "UpDownCounter {connection}", transportActiveConns.ToString())
               .AddRow("Beskar.Networking.Transport", "beskar.transport.connections.opened/closed", "Counter {connection}", $"Opened: {transportOpenedConns} | Closed: {transportClosedConns}")
               .AddRow("Beskar.Networking.Transport", "beskar.transport.bytes.sent/received", "Counter By", $"Sent: {transportBytesSent:N0} B | Recv: {transportBytesRecv:N0} B")
               .Render();
         }
      }
   }

   private static void LogChaosEvent(string source, string type, string message, ConsoleColor color)
   {
      if (_concurrentClients > 1)
      {
         return;
      }
      lock (LogLock)
      {
         var tagColorName = color.ToString();
         var time = DateTime.Now.ToString("HH:mm:ss");
         ConsoleRender.WriteMarkupLine($"[darkgray][{time}][/] [[{tagColorName}]{source,-6}[/]] [[yellow]{type,-9}[/]] {message}");
      }
   }

   private static void SafeClear()
   {
      try
      {
         if (!Console.IsInputRedirected && !Console.IsOutputRedirected)
         {
            Console.Clear();
         }
      }
      catch
      {
         // Ignored
      }
   }
}
