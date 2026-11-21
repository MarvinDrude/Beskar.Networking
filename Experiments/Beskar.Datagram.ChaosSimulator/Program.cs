using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net;
using System.Text;
using Beskar.Networking.Datagram;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Telemetry;
using Beskar.Networking.Datagram.Transport;
using Beskar.Utilities.Console.Rendering;
using Beskar.Utilities.Tracing;

namespace Beskar.Datagram.ChaosSimulator;

public static class Program
{
   internal static readonly ConcurrentDictionary<string, long> TelemetryGauges = new();
   internal static readonly ConcurrentDictionary<string, long> TelemetryCounters = new();
   private static readonly MeterListener MeterListener = new();
   private static readonly Lock RttHistLock = new();
   private static long _rttSampleCount;
   private static double _rttSampleSum;
   private static double _rttSampleMax;

   internal static readonly Lock LogLock = new();

   // Configuration
   internal static SimulatorProfile Profile { get; set; } = SimulatorProfile.StandardChaos;
   internal static TransportBackend Backend { get; set; } = TransportBackend.RealUdpSockets;
   internal static DatagramSecurityMode SecurityMode { get; set; } = DatagramSecurityMode.Aead;
   internal static int TargetConcurrentClients { get; set; } = 40;
   internal static int StatsIntervalSeconds { get; set; } = 5;
   internal static int? AutoStopSeconds { get; set; }

   // Server counters
   internal static long ServerDisconnectsTotal;
   internal static long ServerDisconnectsGraceful;
   internal static long ServerDisconnectsTimeout;
   internal static long ServerDisconnectsMaxRetries;
   internal static long ServerMigrationsObserved;
   internal static long ServerBroadcastsSent;
   internal static long ServerRecvCh0Ordered;
   internal static long ServerRecvCh1Unordered;
   internal static long ServerRecvCh2UnrelSeq;
   internal static long ServerRecvCh3UnrelUnord;
   internal static long ServerRecvCh4RelSeq;
   internal static long DataIntegrityErrors;

   // Client counters
   internal static long ClientConnectAttempts;
   internal static long ClientConnectSuccess;
   internal static long ClientConnectFailExpected;
   internal static long ClientConnectTimeouts;
   internal static long ClientConnectFailUnexpected;
   internal static long ClientMessagesSentTotal;
   internal static long ClientSentCh0Ordered;
   internal static long ClientSentCh1Unordered;
   internal static long ClientSentCh2UnrelSeq;
   internal static long ClientSentCh3UnrelUnord;
   internal static long ClientSentCh4RelSeq;
   internal static long ClientSendQueueFull;
   internal static long ClientFragmentsSent;
   internal static long ClientMessagesReceived;
   internal static long ClientMigrationsTriggered;
   internal static long ClientFuzzPacketsSent;

   private static readonly byte[] SharedPsk = "beskar-datagram-chaos-psk-2026"u8.ToArray();

   private sealed class PeerVerifierState
   {
      public int NextExpectedCh0Seq;
      public int LastCh2Seq = -1;
   }

   public static async Task Main(string[] args)
   {
      TraceLogger.IsEnabled = false;
      ParseCliArgs(args);
      SetupMetricsListener();

      try { Console.Clear(); } catch { }

      ConsoleRender.DrawHeader(
         "BESKAR DATAGRAM CHAOS SIMULATOR",
         "Stress testing channels, AEAD encryption, allocations, GC, and metrics over time");

      if (args.Length == 0 && !Console.IsInputRedirected)
      {
         Console.WriteLine("Select Simulator Profile:");
         Console.WriteLine("  1. Standard Chaos (Mixed player roles, fragments, NAT migrations, abrupt crashes, fuzzing)");
         Console.WriteLine("  2. High Throughput / Low Churn Soak (Sustained 60Hz steady-state allocation & GC test)");
         Console.WriteLine("  3. Extreme Packet Loss & Reorder Storm (20% drop, 25% reorder, 15% dup, 3% corrupt)");
         Profile = (SimulatorProfile)PromptInt("Profile", (int)Profile);

         if (Profile == SimulatorProfile.ExtremePacketLossStorm)
         {
            Backend = TransportBackend.SimulatedChaosNetwork;
            ConsoleRender.Info("Extreme Packet Loss Storm automatically selects Simulated Chaos Network.");
         }
         else
         {
            Console.WriteLine("Select Transport Backend:");
            Console.WriteLine("  1. Real OS UDP Sockets (IPAddress.Loopback)");
            Console.WriteLine("  2. Simulated Chaos Network (Packet loss, jitter, reordering, NAT rebinding)");
            Backend = (TransportBackend)PromptInt("Backend", (int)Backend);
         }

         Console.WriteLine("Select Security Mode:");
         Console.WriteLine("  0. None (Raw plaintext)");
         Console.WriteLine("  1. Crc32C (Integrity only)");
         Console.WriteLine("  2. Aead (ECDH P-256 + HKDF + AesGcm/ChaCha20-Poly1305 + PSK)");
         SecurityMode = (DatagramSecurityMode)PromptInt("Security Mode", (int)SecurityMode);

         TargetConcurrentClients = PromptInt("Target concurrent clients", TargetConcurrentClients);
         StatsIntervalSeconds = PromptInt("Stats reporting interval (seconds)", StatsIntervalSeconds);
         Console.WriteLine();
      }
      else if (Profile == SimulatorProfile.ExtremePacketLossStorm)
      {
         Backend = TransportBackend.SimulatedChaosNetwork;
      }

      ConsoleRender.Info($"Profile: {Profile} | Backend: {Backend} | Security: {SecurityMode}");
      ConsoleRender.Info($"Target Concurrent Clients: {TargetConcurrentClients} | Report Interval: {StatsIntervalSeconds}s");

      SimulatedNetworkHub? simHub = null;
      if (Backend == TransportBackend.SimulatedChaosNetwork)
      {
         var chaosProfile = Profile switch
         {
            SimulatorProfile.ExtremePacketLossStorm => new SimulatedChaosProfile
            {
               PacketDropRate = 0.20,
               PacketDuplicateRate = 0.15,
               ReorderRate = 0.25,
               DataCorruptionRate = 0.03,
               MinLatency = TimeSpan.FromMilliseconds(10),
               MaxLatency = TimeSpan.FromMilliseconds(65),
               ReorderExtraDelay = TimeSpan.FromMilliseconds(40)
            },
            SimulatorProfile.HighThroughputSoak => new SimulatedChaosProfile
            {
               PacketDropRate = 0.01,
               MinLatency = TimeSpan.FromMilliseconds(5),
               MaxLatency = TimeSpan.FromMilliseconds(15)
            },
            _ => new SimulatedChaosProfile
            {
               PacketDropRate = 0.06,
               PacketDuplicateRate = 0.04,
               ReorderRate = 0.10,
               DataCorruptionRate = 0.01,
               MinLatency = TimeSpan.FromMilliseconds(5),
               MaxLatency = TimeSpan.FromMilliseconds(35)
            }
         };

         simHub = new SimulatedNetworkHub(seed: 2026, chaos: chaosProfile);
      }

      var serverOptions = BuildBaseOptions(isServer: true);
      var serverEndPoint = new IPEndPoint(IPAddress.Loopback, 0);
      var serverTransport = simHub is not null ? (IDatagramTransport)simHub.CreateTransport() : null;

      await using var server = new DatagramHost(serverEndPoint, serverOptions, serverTransport, simHub?.Time);
      RegisterServerHandlers(server);
      server.Start();

      var boundServerEp = server.LocalEndPoint!;
      ConsoleRender.Success($"DatagramHost Server listening on {boundServerEp}");

      using var cts = new CancellationTokenSource();

      // Background simulated-clock driver when using SimulatedNetworkHub
      Task? simPumpTask = null;
      if (simHub is not null)
      {
         simPumpTask = Task.Run(() => RunSimulatedHubClockAsync(simHub, cts.Token));
      }

      // Server 30Hz broadcast loop (simulates authoritative live-game server broadcasting snapshots & turns)
      var serverBroadcastTask = Task.Run(() => RunServerBroadcastLoopAsync(server, cts.Token));

      // Launch chaos client workers
      ConsoleRender.Info($"Launching {TargetConcurrentClients} concurrent client workers...");
      var clientTasks = new List<Task>(TargetConcurrentClients);
      for (var i = 0; i < TargetConcurrentClients; i++)
      {
         var idx = i;
         clientTasks.Add(Task.Run(() => RunClientWorkerLoopAsync(idx, boundServerEp, serverOptions, simHub, cts.Token)));
      }

      var reporterTask = Task.Run(() => StatsReporter.RunStatsReporterAsync(server, cts.Token));

      if (AutoStopSeconds.HasValue)
      {
         ConsoleRender.Info($"Running unattended for {AutoStopSeconds.Value} seconds...");
         try { await Task.Delay(AutoStopSeconds.Value * 1000, cts.Token); } catch (OperationCanceledException) { }
      }
      else if (Console.IsInputRedirected)
      {
         ConsoleRender.Info("Input redirected; running for 12 seconds...");
         try { await Task.Delay(12_000, cts.Token); } catch (OperationCanceledException) { }
      }
      else
      {
         ConsoleRender.Info("Simulator is running. Press Enter to stop...");
         Console.ReadLine();
      }

      ConsoleRender.Warning("Shutting down Datagram Chaos Simulator...");
      await cts.CancelAsync();

      try { await Task.WhenAll(clientTasks); } catch { }
      try { await serverBroadcastTask; } catch { }
      if (simPumpTask is not null)
      {
         try { await simPumpTask; } catch { }
      }
      try { await reporterTask; } catch { }

      await server.DisposeAsync();
      MeterListener.Dispose();

      var finalErrors = Interlocked.Read(ref DataIntegrityErrors);
      if (finalErrors == 0)
      {
         ConsoleRender.Success("Datagram Chaos Simulator stopped cleanly with 0 data integrity errors.");
      }
      else
      {
         ConsoleRender.Error($"Datagram Chaos Simulator stopped with {finalErrors} data integrity error(s)!");
      }
   }

   private static DatagramOptions BuildBaseOptions(bool isServer)
   {
      var opts = new DatagramOptions
      {
         MaxDatagramSize = 1200,
         MaxPeers = Math.Max(256, TargetConcurrentClients * 2),
         DispatchMode = DatagramEventDispatchMode.Async,
         ManualTick = false,
         TickInterval = TimeSpan.FromMilliseconds(10),
         HandshakeRetryInterval = TimeSpan.FromMilliseconds(150),
         ConnectTimeout = TimeSpan.FromSeconds(4),
         KeepAliveInterval = TimeSpan.FromMilliseconds(250),
         PeerTimeout = TimeSpan.FromSeconds(4),
         MaxReliableRetryDuration = TimeSpan.FromSeconds(8),
         TimeSyncInterval = TimeSpan.FromSeconds(1),
         MaxBytesPerSecondPerPeer = isServer ? 8 * 1024 * 1024 : 2 * 1024 * 1024,
         PacingBurstBytes = 64 * 1024,
         MaxQueuedEvents = 32_768,
         Security = new DatagramSecurityOptions
         {
            Mode = SecurityMode,
            AeadCipher = DatagramAeadCipher.Auto,
            PreSharedKey = SecurityMode == DatagramSecurityMode.Aead ? SharedPsk : null,
            PadConnectRequests = true,
            AllowEndpointMigration = true
         }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered, priority: 255, windowSize: 512, maxSendQueueDepth: 2048, allowFragmentation: true)
      .AddChannel(1, DatagramDeliveryMode.ReliableUnordered, priority: 220, windowSize: 512, maxSendQueueDepth: 2048, allowFragmentation: true)
      .AddChannel(2, DatagramDeliveryMode.UnreliableSequenced, priority: 150, allowFragmentation: false)
      .AddChannel(3, DatagramDeliveryMode.UnreliableUnordered, priority: 80, allowFragmentation: false)
      .AddChannel(4, DatagramDeliveryMode.ReliableSequenced, priority: 180, windowSize: 256, maxSendQueueDepth: 512, allowFragmentation: true);

      return opts;
   }

   internal static DatagramOptions CloneClientOptions(DatagramOptions _) => BuildBaseOptions(isServer: false);

   private static void RegisterServerHandlers(DatagramHost server)
   {
      server.Events.ValidateConnection = ctx =>
      {
         var name = Encoding.UTF8.GetString(ctx.ConnectMetadata.Span);
         if (name.StartsWith("banned-", StringComparison.Ordinal))
         {
            ctx.Reject("Client banned by chaos policy"u8);
            return ValueTask.CompletedTask;
         }

         ctx.PeerTag = new PeerVerifierState();
         return ValueTask.CompletedTask;
      };

      server.Events.PeerDisconnected = (_, reason, _, _) =>
      {
         Interlocked.Increment(ref ServerDisconnectsTotal);
         switch (reason)
         {
            case DatagramDisconnectReason.GracefulLocal:
            case DatagramDisconnectReason.GracefulRemote:
               Interlocked.Increment(ref ServerDisconnectsGraceful);
               break;
            case DatagramDisconnectReason.Timeout:
               Interlocked.Increment(ref ServerDisconnectsTimeout);
               break;
            case DatagramDisconnectReason.MaxRetriesExceeded:
               Interlocked.Increment(ref ServerDisconnectsMaxRetries);
               break;
         }
         return ValueTask.CompletedTask;
      };

      server.Events.PeerMigrated = (_, _, _, _) =>
      {
         Interlocked.Increment(ref ServerMigrationsObserved);
         return ValueTask.CompletedTask;
      };

      server.Events.MessageReceived = (peer, channelId, _, payload, _) =>
      {
         var verifier = peer.Tag as PeerVerifierState;
         var span = payload.Span;

         switch (channelId)
         {
            case 0:
               Interlocked.Increment(ref ServerRecvCh0Ordered);
               if (verifier is not null)
               {
                  if (!ClientBehaviors.VerifyPayload(span, out var seq) || seq != verifier.NextExpectedCh0Seq)
                  {
                     Interlocked.Increment(ref DataIntegrityErrors);
                  }
                  else
                  {
                     verifier.NextExpectedCh0Seq++;
                  }
               }
               break;

            case 1:
               Interlocked.Increment(ref ServerRecvCh1Unordered);
               if (!ClientBehaviors.VerifyPayload(span, out _))
               {
                  Interlocked.Increment(ref DataIntegrityErrors);
               }
               break;

            case 2:
               Interlocked.Increment(ref ServerRecvCh2UnrelSeq);
               if (verifier is not null && span.Length >= 4)
               {
                  var s = BinaryPrimitives.ReadInt32LittleEndian(span);
                  if (s <= verifier.LastCh2Seq)
                  {
                     Interlocked.Increment(ref DataIntegrityErrors);
                  }
                  verifier.LastCh2Seq = s;
               }
               break;

            case 3:
               Interlocked.Increment(ref ServerRecvCh3UnrelUnord);
               break;

            case 4:
               Interlocked.Increment(ref ServerRecvCh4RelSeq);
               break;
         }

         return ValueTask.CompletedTask;
      };
   }

   private static async Task RunServerBroadcastLoopAsync(DatagramHost server, CancellationToken ct)
   {
      using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(33)); // ~30Hz broadcast tick
      var tick = 0;
      var payload = new byte[48];

      try
      {
         while (await timer.WaitForNextTickAsync(ct))
         {
            var peerCount = server.PeerCount;
            if (peerCount == 0) continue;

            tick++;
            BinaryPrimitives.WriteInt32LittleEndian(payload, tick);

            // 30Hz UnreliableSequenced world snapshot
            server.Broadcast(2, payload);
            Interlocked.Add(ref ServerBroadcastsSent, peerCount);

            // Periodic ReliableSequenced HUD update
            if (tick % 10 == 0)
            {
               server.Broadcast(4, payload.AsSpan(0, 16));
               Interlocked.Add(ref ServerBroadcastsSent, peerCount);
            }

            // Periodic ReliableOrdered turn event
            if (tick % 15 == 0)
            {
               server.Broadcast(0, payload.AsSpan(0, 24));
               Interlocked.Add(ref ServerBroadcastsSent, peerCount);
            }
         }
      }
      catch (OperationCanceledException)
      {
      }
   }

   private static async Task RunClientWorkerLoopAsync(
      int workerIndex,
      IPEndPoint serverEp,
      DatagramOptions baseOptions,
      SimulatedNetworkHub? simHub,
      CancellationToken ct)
   {
      // Stagger startup slightly to avoid thundering-herd CPU spike at t=0
      try { await Task.Delay(Random.Shared.Next(10, 400), ct); } catch (OperationCanceledException) { return; }

      while (!ct.IsCancellationRequested)
      {
         var behavior = PickBehavior();

         if (behavior == ClientBehavior.AuthRejectOrFuzz)
         {
            try
            {
               await ClientBehaviors.ExecuteAuthRejectOrFuzzAsync(serverEp, baseOptions, simHub, ct);
               await Task.Delay(Random.Shared.Next(150, 400), ct);
            }
            catch (OperationCanceledException) { break; }
            catch { }
            continue;
         }

         var clientOpts = CloneClientOptions(baseOptions);
         var simTransport = simHub is not null ? simHub.CreateTransport() : null;
         var host = new DatagramHost(
            new IPEndPoint(IPAddress.Loopback, 0),
            clientOpts,
            simTransport,
            simHub?.Time);

         host.Events.MessageReceived = (_, _, _, _, _) =>
         {
            Interlocked.Increment(ref ClientMessagesReceived);
            return ValueTask.CompletedTask;
         };

         try
         {
            host.Start();
            Interlocked.Increment(ref ClientConnectAttempts);

            var meta = Encoding.UTF8.GetBytes($"worker-{workerIndex}-{behavior}");
            var peer = await host.ConnectAsync(serverEp, meta, ct);
            Interlocked.Increment(ref ClientConnectSuccess);

            var stayDurationMs = Profile == SimulatorProfile.HighThroughputSoak
               ? Random.Shared.Next(20_000, 45_000)
               : Random.Shared.Next(3_000, 9_000);

            switch (behavior)
            {
               case ClientBehavior.ActionPlayer:
                  await ClientBehaviors.ExecuteActionPlayerAsync(peer, stayDurationMs, ct);
                  peer.Disconnect("graceful"u8);
                  await Task.Delay(50, ct);
                  break;

               case ClientBehavior.StateSyncFragmenter:
                  await ClientBehaviors.ExecuteStateSyncFragmenterAsync(peer, stayDurationMs, ct);
                  peer.Disconnect("graceful"u8);
                  await Task.Delay(50, ct);
                  break;

               case ClientBehavior.RoamingMobile:
                  await ClientBehaviors.ExecuteRoamingMobileAsync(peer, simHub, simTransport, stayDurationMs, ct);
                  peer.Disconnect("graceful"u8);
                  await Task.Delay(50, ct);
                  break;

               case ClientBehavior.KeepAliveIdler:
                  await ClientBehaviors.ExecuteKeepAliveIdlerAsync(peer, stayDurationMs, ct);
                  peer.Disconnect("graceful"u8);
                  await Task.Delay(50, ct);
                  break;

               case ClientBehavior.FlakyCrasher:
                  await ClientBehaviors.ExecuteFlakyCrasherAsync(peer, ct);
                  // Intentional abrupt crash (skip graceful Disconnect!)
                  break;
            }
         }
         catch (OperationCanceledException)
         {
            break;
         }
         catch (TimeoutException)
         {
            Interlocked.Increment(ref ClientConnectTimeouts);
            if (Profile != SimulatorProfile.ExtremePacketLossStorm)
               Interlocked.Increment(ref ClientConnectFailUnexpected);
         }
         catch (Exception)
         {
            Interlocked.Increment(ref ClientConnectFailUnexpected);
         }
         finally
         {
            await host.DisposeAsync();
         }

         if (!ct.IsCancellationRequested)
         {
            try { await Task.Delay(Random.Shared.Next(50, 250), ct); } catch (OperationCanceledException) { break; }
         }
      }
   }

   private static ClientBehavior PickBehavior()
   {
      var roll = Random.Shared.Next(100);
      if (Profile == SimulatorProfile.HighThroughputSoak)
      {
         return roll switch
         {
            < 65 => ClientBehavior.ActionPlayer,
            < 90 => ClientBehavior.StateSyncFragmenter,
            _ => ClientBehavior.KeepAliveIdler
         };
      }

      return roll switch
      {
         < 35 => ClientBehavior.ActionPlayer,
         < 55 => ClientBehavior.StateSyncFragmenter,
         < 70 => ClientBehavior.RoamingMobile,
         < 85 => ClientBehavior.FlakyCrasher,
         < 95 => ClientBehavior.KeepAliveIdler,
         _ => ClientBehavior.AuthRejectOrFuzz
      };
   }

   private static async Task RunSimulatedHubClockAsync(SimulatedNetworkHub hub, CancellationToken ct)
   {
      using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(5));
      var sw = Stopwatch.StartNew();
      var last = sw.Elapsed;

      try
      {
         while (await timer.WaitForNextTickAsync(ct))
         {
            var now = sw.Elapsed;
            var delta = now - last;
            last = now;
            if (delta > TimeSpan.Zero)
            {
               hub.AdvanceAndDeliver(delta);
            }
         }
      }
      catch (OperationCanceledException)
      {
      }
   }

   private static void SetupMetricsListener()
   {
      MeterListener.InstrumentPublished = (instrument, listener) =>
      {
         if (instrument.Meter.Name == DatagramMetrics.MeterName)
         {
            listener.EnableMeasurementEvents(instrument);
         }
      };

      MeterListener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
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

      MeterListener.SetMeasurementEventCallback<double>((instrument, measurement, _, _) =>
      {
         if (instrument.Name == "beskar.datagram.rtt")
         {
            lock (RttHistLock)
            {
               _rttSampleCount++;
               _rttSampleSum += measurement;
               if (measurement > _rttSampleMax) _rttSampleMax = measurement;
            }
         }
      });

      MeterListener.Start();
   }

   internal static (long Count, double AvgMs, double MaxMs) ReadAndResetRttHistogram()
   {
      lock (RttHistLock)
      {
         var count = _rttSampleCount;
         var avg = count > 0 ? _rttSampleSum / count : 0.0;
         var max = _rttSampleMax;
         _rttSampleCount = 0;
         _rttSampleSum = 0;
         _rttSampleMax = 0;
         return (count, avg, max);
      }
   }

   private static void ParseCliArgs(string[] args)
   {
      for (var i = 0; i < args.Length - 1; i++)
      {
         switch (args[i].ToLowerInvariant())
         {
            case "--profile" when int.TryParse(args[i + 1], out var p) && Enum.IsDefined((SimulatorProfile)p):
               Profile = (SimulatorProfile)p;
               i++;
               break;
            case "--backend" when int.TryParse(args[i + 1], out var b) && Enum.IsDefined((TransportBackend)b):
               Backend = (TransportBackend)b;
               i++;
               break;
            case "--security" when int.TryParse(args[i + 1], out var s) && Enum.IsDefined((DatagramSecurityMode)s):
               SecurityMode = (DatagramSecurityMode)s;
               i++;
               break;
            case "--clients" when int.TryParse(args[i + 1], out var c) && c > 0:
               TargetConcurrentClients = c;
               i++;
               break;
            case "--interval" when int.TryParse(args[i + 1], out var iv) && iv > 0:
               StatsIntervalSeconds = iv;
               i++;
               break;
            case "--duration" when int.TryParse(args[i + 1], out var dur) && dur > 0:
               AutoStopSeconds = dur;
               i++;
               break;
         }
      }
   }

   private static int PromptInt(string prompt, int defaultValue)
   {
      Console.Write($"{prompt} [default: {defaultValue}]: ");
      var input = Console.ReadLine();
      if (string.IsNullOrWhiteSpace(input)) return defaultValue;
      return int.TryParse(input, out var value) ? value : defaultValue;
   }
}
