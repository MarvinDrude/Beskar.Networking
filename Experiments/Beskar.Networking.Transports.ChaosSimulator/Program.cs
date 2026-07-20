using System;
using System.Collections.Concurrent;
using System.IO;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
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
   private static bool _clientIsConnected;

   private static readonly Lock LogLock = new();

   public static async Task Main(string[] args)
   {
      // Disable noisy internal trace logging to clean up output
      TraceLogger.IsEnabled = false;

      SafeClear();

      ConsoleRender.DrawHeader("BESKAR TRANSPORT CHAOS SIMULATOR",
         "Hostile testing framework for lower-level transport abstraction interfaces");

      var transportOption = 1;
      var chaosOption = 6; // Default to Total Chaos in automated runs

      if (args.Length >= 2 && int.TryParse(args[0], out var tArg) && int.TryParse(args[1], out var cArg))
      {
         transportOption = tArg;
         chaosOption = cArg;
      }
      else if (Console.IsInputRedirected)
      {
         transportOption = 1; // TCP
         chaosOption = 6;     // Total Chaos
      }
      else
      {
         // 1. Select Transport
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

         // Check QUIC compatibility if selected
         if (transportOption == 4 && !QuicConnection.IsSupported)
         {
            ConsoleRender.Error("QUIC is not supported on this platform/OS. Falling back to TCP.");
            transportOption = 1;
            await Task.Delay(1500);
         }

         // 2. Select Chaos Configuration Mode
         SafeClear();
         ConsoleRender.DrawHeader("BESKAR TRANSPORT CHAOS SIMULATOR", "Choose Chaos Profile");
         Console.WriteLine("  1. Clean Link (No injected failures/baseline)");
         Console.WriteLine("  2. Flaky Link (High packet drops, session disconnects, connect failures)");
         Console.WriteLine("  3. High Latency Link (Adds random delays of 100-300ms on all reads and writes)");
         Console.WriteLine("  4. Corrupted Link (Adds random 3% payload bit-flip corruption)");
         Console.WriteLine("  5. Throttled Pipe (Enforces 50 KB/s bandwidth limits and minor latency)");
         Console.WriteLine("  6. TOTAL CHAOS (Runs all profiles concurrently - disconnects, drops, latency, throttling, corruption)");
         Console.Write("\nSelect Option (1-6): ");
         var chaosInput = Console.ReadLine();
         if (int.TryParse(chaosInput, out var cOpt) && cOpt >= 1 && cOpt <= 6)
         {
            chaosOption = cOpt;
         }
      }

      var options = chaosOption switch
      {
         1 => ChaosOptions.Clean,
         2 => ChaosOptions.Flaky,
         3 => ChaosOptions.Latent,
         4 => ChaosOptions.Corrupt,
         5 => ChaosOptions.Throttled,
         6 => ChaosOptions.TotalChaos,
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
            listenerAddress = new NamedPipeEndPoint(".", $"chaos-pipe-{Guid.NewGuid():N}");
            break;
         case 7: // Memory
            listenerAddress = new MemoryEndPoint($"chaos-mem-{Guid.NewGuid():N}");
            break;
         default: // TCP, WS, UDP, QUIC
            listenerAddress = new IPEndPoint(IPAddress.Loopback, 0);
            break;
      }

      // Create raw inner client & listener
      INetworkListener rawListener;
      INetworkClient rawClient;

      X509Certificate2? quicCert = null;

      try
      {
         switch (transportOption)
         {
            case 1: // TCP
               var tcpOptions = new TcpTransportOptions();
               rawListener = new TcpNetworkListener(listenerAddress, tcpOptions);
               rawClient = new TcpNetworkClient(tcpOptions);
               break;

            case 2: // WS
               var wsOptions = new WsTransportOptions();
               rawListener = new WsNetworkListener(listenerAddress, wsOptions);
               rawClient = new WsNetworkClient(wsOptions);
               break;

            case 3: // UDP
               var udpOptions = new UdpTransportOptions();
               rawListener = new UdpNetworkListener(listenerAddress, udpOptions);
               rawClient = new UdpNetworkClient(udpOptions);
               break;

            case 4: // QUIC
               quicCert = CertificateUtility.GenerateSelfSignedCertificate();
               var quicOptions = new QuicTransportOptions
               {
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
               rawClient = new QuicNetworkClient(quicOptions);
               break;

            case 5: // UDS
               var udsOptions = new UdsTransportOptions();
               rawListener = new UdsNetworkListener(listenerAddress, udsOptions);
               rawClient = new UdsNetworkClient(udsOptions);
               break;

            case 6: // Named Pipes
               var npOptions = new NamedPipeTransportOptions();
               rawListener = new NamedPipeNetworkListener(listenerAddress, npOptions);
               rawClient = new NamedPipeNetworkClient(npOptions);
               break;

            case 7: // Memory
               var memOptions = new MemoryTransportOptions();
               rawListener = new MemoryNetworkListener((MemoryEndPoint)listenerAddress, memOptions);
               rawClient = new MemoryNetworkClient(memOptions);
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

      // Wrap them in our Chaos Decorators
      var listener = new ChaosNetworkListener(rawListener, options);
      var client = new ChaosNetworkClient(rawClient, options);

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

      // Start active client scenario loop
      var clientTask = Task.Run(() => ClientScenarioLoopAsync(client, listener.LocalAddress, cts.Token));

      // Start statistics dashboard loop
      var statsTask = Task.Run(() => DisplayDashboardLoopAsync(options, cts.Token));

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
         await Task.WhenAll(serverTask, clientTask, statsTask);
      }
      catch
      {
         // Ignored
      }

      // Cleanup
      await client.DisposeAsync();
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

   private static async Task ClientScenarioLoopAsync(INetworkClient client, EndPoint endPoint, CancellationToken ct)
   {
      var currentSeq = 0;
      var payload = new byte[512];
      Random.Shared.NextBytes(payload);

      while (!ct.IsCancellationRequested)
      {
         Interlocked.Increment(ref _clientConnectionAttempts);
         _clientIsConnected = false;

         var connectResult = await client.ConnectAsync(endPoint, ct);
         if (connectResult.Failed)
         {
            Interlocked.Increment(ref _clientConnectionFailures);
            LogChaosEvent("CLIENT", "CONN_FAIL", $"Failed to connect: {connectResult.Error.Message}", ConsoleColor.DarkGray);
            await Task.Delay(1000, ct);
            continue;
         }

         Interlocked.Increment(ref _clientConnectionsEstablished);
         _clientIsConnected = true;
         LogChaosEvent("CLIENT", "CONN_OK", "Established connection successfully.", ConsoleColor.Green);

         var session = connectResult.Success;
         try
         {
            var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
            if (streamResult.Failed)
            {
               LogChaosEvent("CLIENT", "STRM_FAIL", $"Failed to open stream: {streamResult.Error.Message}", ConsoleColor.Red);
               throw new InvalidOperationException();
            }

            var stream = streamResult.Success;
            await using (stream)
            {
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
            _clientIsConnected = false;
            LogChaosEvent("CLIENT", "CONN_LOST", $"Session dropped: {ex.Message}", ConsoleColor.Red);
         }
         finally
         {
            await session.DisposeAsync();
         }

         // Wait briefly before reconnecting
         await Task.Delay(Random.Shared.Next(1000, 2000), ct);
      }
   }

   private static async Task DisplayDashboardLoopAsync(ChaosOptions options, CancellationToken ct)
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
            Console.WriteLine($"Client State:       {(Volatile.Read(ref _clientIsConnected) ? "CONNECTED" : "DISCONNECTED")}");
            Console.WriteLine($"Active Server Sess: {Volatile.Read(ref _activeServerSessions)}");
            Console.WriteLine($"Connection Stats:   Attempts={Volatile.Read(ref _clientConnectionAttempts)} Failures={Volatile.Read(ref _clientConnectionFailures)} Established={Volatile.Read(ref _clientConnectionsEstablished)} Lost={Volatile.Read(ref _clientConnectionsLost)}");
            Console.WriteLine($"Data Sent/Recv:     Sent={Volatile.Read(ref _packetsSent)} Recv={Volatile.Read(ref _packetsReceived)} Speed={speedKB:F1} KB/s");
            Console.WriteLine($"Fault Metrics:      ChecksumFailures={Volatile.Read(ref _checksumFailures)} Gaps(Drops)={Volatile.Read(ref _droppedPackets)} OutOfOrder={Volatile.Read(ref _outOfOrderPackets)}");
            Console.WriteLine($"Latency (ms):       Min={minLat} Max={maxLat} Avg={avgLatency:F1}");
            Console.WriteLine("[---------------------------------------]");
         }
      }
   }

   private static void LogChaosEvent(string source, string type, string message, ConsoleColor color)
   {
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
