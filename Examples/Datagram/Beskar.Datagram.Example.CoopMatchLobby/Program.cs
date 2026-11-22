using System.Buffers.Binary;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Net;
using System.Text;
using Beskar.Networking.Datagram;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Nat;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Telemetry;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Datagram.Example.CoopMatchLobby;

internal sealed class PlayerMetricsTracker(string name)
{
   public string Name { get; } = name;
   public bool MapVerified { get; set; }

   public List<string> OrderedPhases { get; } = [];
   public List<string> SquadWhispers { get; } = [];

   public HashSet<int> UniqueLootIds { get; } = [];

   public List<int> BossHpUpdates { get; } = [];
   public List<uint> PositionTicks { get; } = [];

   public int FxReceived { get; set; }
}

public static class Program
{
   public static async Task Main()
   {
      Console.WriteLine("======================================================================");
      Console.WriteLine("  Beskar.Networking.Datagram - Co-op Match Lobby (Intermediate Demo)");
      Console.WriteLine("  All 5 Channels | Fragmentation | Chaos | Migration | Metrics | NAT");
      Console.WriteLine("======================================================================\n");

      await RunSimulatedChaosMatchAsync();
      await RunRealUdpNatHolePunchDemoAsync();

      Console.WriteLine("\nAll stages completed cleanly.");
   }

   /// <summary>
   /// Stages 1-4: Runs an authoritative server + 3 squadmates over a deterministic WAN chaos fabric
   /// (12% packet loss, 18% reorder, 8% dup, 2% bit corruption, 10-45ms jitter) with Poll + ManualTick.
   /// </summary>
   private static async Task RunSimulatedChaosMatchAsync()
   {
      // Subscribe to native System.Diagnostics.Metrics instruments
      long totalDatagramsSent = 0;
      long totalDatagramsRecv = 0;
      long totalRetransmits = 0;
      long totalSecurityRejects = 0;

      using var meterListener = new MeterListener();
      meterListener.InstrumentPublished = (inst, listener) =>
      {
         if (inst.Meter.Name == DatagramMetrics.MeterName)
         {
            listener.EnableMeasurementEvents(inst);
         }
      };
      meterListener.SetMeasurementEventCallback<long>((inst, value, _, _) =>
      {
         switch (inst.Name)
         {
            case "beskar.datagram.packets.sent": Interlocked.Add(ref totalDatagramsSent, value); break;
            case "beskar.datagram.packets.received": Interlocked.Add(ref totalDatagramsRecv, value); break;
            case "beskar.datagram.messages.retransmitted": Interlocked.Add(ref totalRetransmits, value); break;
            case "beskar.datagram.security.rejected": Interlocked.Add(ref totalSecurityRejects, value); break;
         }
      });
      meterListener.Start();

      // Virtual clocks offset by +180ms to demonstrate built-in NTP TimeSyncPing/Pong clock estimation
      var clientClock = new SimulatedTimeProvider(new DateTimeOffset(2026, 11, 22, 12, 0, 0, TimeSpan.Zero));
      var serverClock = new SimulatedTimeProvider(new DateTimeOffset(2026, 11, 22, 12, 0, 0, 180, TimeSpan.Zero));

      var chaos = new SimulatedChaosProfile
      {
         PacketDropRate = 0.12,
         PacketDuplicateRate = 0.08,
         ReorderRate = 0.18,
         DataCorruptionRate = 0.02,
         MinLatency = TimeSpan.FromMilliseconds(10),
         MaxLatency = TimeSpan.FromMilliseconds(45),
         ReorderExtraDelay = TimeSpan.FromMilliseconds(40)
      };

      var hub = new SimulatedNetworkHub(seed: 2026, chaos: chaos, time: clientClock);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9400);

      await using var server = new DatagramHost(
         serverEp,
         LobbyProtocol.CreateMatchOptions(manualTick: true, DatagramEventDispatchMode.Poll),
         hub.CreateTransport(),
         serverClock);

      // Stage 1: Authenticate incoming peers with AEAD + PSK + ValidateConnection
      Console.WriteLine("[Stage 1] AEAD + PSK Handshake & ValidateConnection Admission");

      server.Events.ValidateConnection = ctx =>
      {
         var name = Encoding.UTF8.GetString(ctx.ConnectMetadata.Span).Trim();
         if (name.StartsWith("Cheater", StringComparison.OrdinalIgnoreCase))
         {
            ctx.Reject("Account suspended by anti-cheat"u8);
            return ValueTask.CompletedTask;
         }

         ctx.PeerTag = name;
         return ValueTask.CompletedTask;
      };
      server.Start();

      var playerNames = new[] { "Valkyrie", "Ranger", "Medic" };
      var clientTransports = new SimulatedDatagramTransport[playerNames.Length];
      var clients = new DatagramHost[playerNames.Length];
      var clientPeers = new DatagramPeer[playerNames.Length];
      var trackers = new PlayerMetricsTracker[playerNames.Length];

      for (var i = 0; i < playerNames.Length; i++)
      {
         clientTransports[i] = hub.CreateTransport();
         var bindEp = i == 1
            ? new IPEndPoint(IPAddress.Parse("192.168.1.42"), 50010) // Ranger starts on Wi-Fi
            : new IPEndPoint(IPAddress.Loopback, 0);

         clients[i] = new DatagramHost(
            bindEp,
            LobbyProtocol.CreateMatchOptions(manualTick: true, DatagramEventDispatchMode.Poll),
            clientTransports[i],
            clientClock);
         clients[i].Start();
         trackers[i] = new PlayerMetricsTracker(playerNames[i]);
      }

      await using var bannedClient = new DatagramHost(
         new IPEndPoint(IPAddress.Loopback, 0),
         LobbyProtocol.CreateMatchOptions(manualTick: true, DatagramEventDispatchMode.Poll),
         hub.CreateTransport(),
         clientClock);
      bannedClient.Start();

      var connectTasks = playerNames
         .Select((name, idx) => clients[idx].ConnectAsync(serverEp, Encoding.UTF8.GetBytes(name)))
         .ToArray();
      var bannedTask = bannedClient.ConnectAsync(serverEp, "CheaterX"u8.ToArray());

      for (var i = 0; i < 80 && (connectTasks.Any(t => !t.IsCompleted) || !bannedTask.IsCompleted); i++)
      {
         StepNetwork(TimeSpan.FromMilliseconds(25));
      }

      for (var i = 0; i < playerNames.Length; i++)
      {
         clientPeers[i] = await connectTasks[i];
         clientPeers[i].SetProperty("role", playerNames[i]);
      }

      try
      {
         await bannedTask;
      }
      catch (DatagramConnectionRejectedException ex)
      {
         Console.WriteLine($"  Rejected 'CheaterX' as expected: \"{ex.Message}\"");
      }

      var serverPeersByName = new Dictionary<string, DatagramPeer>();
      server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.PeerConnected && evt.Peer.Tag is string name)
         {
            evt.Peer.SetProperty("squad", "Alpha");
            serverPeersByName[name] = evt.Peer;
            Console.WriteLine($"  Accepted Peer #{evt.Peer.PeerId} ({name}) from {evt.Peer.RemoteEndPoint}");
         }
      });
      foreach (var c in clients) c.PollEvents(_ => { });

      // Stage 2: Multi-MTU Fragmented 16 KB Map Sync over Ch 0 (ReliableOrdered)
      Console.WriteLine("\n[Stage 2] Fragmented 16 KB Map Snapshot Transfer Under 12% Loss + 18% Reordering");
      var mapSnapshot = LobbyProtocol.BuildMapSnapshot(16_384, out var expectedMapChecksum);
      server.Broadcast(LobbyProtocol.ChLifecycle, mapSnapshot);

      for (var t = 0; t < 160 && trackers.Any(tr => !tr.MapVerified); t++)
      {
         StepNetwork(TimeSpan.FromMilliseconds(25));
         DrainClients(expectedMapChecksum);
      }

      foreach (var tr in trackers)
      {
         Console.WriteLine($"  {tr.Name,-10} reassembled 16 KB map (checksum match: {tr.MapVerified})");
      }

      // Stage 3: Live 5-Channel Encounter + Targeted SendTo + Mid-Match NAT Migration
      Console.WriteLine("\n[Stage 3] Live 5-Channel Boss Encounter + SendTo Subset + Mid-Match NAT Migration");
      const int totalTicks = 60;
      const int expectedLootCount = 30;
      Span<byte> scratch = stackalloc byte[32];

      var phases = new[] { "Phase 1: Shield Up", "Phase 2: Enrage", "Phase 3: Final Stand", "Victory!" };
      var migrationObserved = false;

      // Send a targeted squad whisper to Valkyrie & Medic only (excluding Ranger) using Host.SendTo
      var squadSubset = new[] { serverPeersByName["Valkyrie"].PeerId, serverPeersByName["Medic"].PeerId };
      server.SendTo(squadSubset, LobbyProtocol.ChLifecycle, LobbyProtocol.BuildTaggedText(LobbyProtocol.OpSquadWhisper, "Focus left flank!"));

      for (var tick = 0; tick < totalTicks; tick++)
      {
         // Mid-match NAT rebind: Ranger switches from Wi-Fi to 5G cellular IP:Port seamlessly!
         if (tick == 30)
         {
            var newCellularEp = hub.MigrateEndpoint(clientTransports[1], new IPEndPoint(IPAddress.Parse("10.88.4.19"), 61200));
            Console.WriteLine($"  [Tick {tick}] Ranger switched network to {newCellularEp} (AEAD endpoint migration)");
         }

         // Ch 0 (ReliableOrdered): Match phase announcements
         if (tick % 15 == 0 && tick / 15 < phases.Length)
         {
            server.Broadcast(LobbyProtocol.ChLifecycle, LobbyProtocol.BuildTaggedText(LobbyProtocol.OpPhaseText, phases[tick / 15]));
         }

         // Ch 1 (ReliableUnordered): Independent loot/crit confirmations (zero HoL blocking)
         if (tick % 2 == 0)
         {
            var lootId = tick / 2;
            var len = LobbyProtocol.WriteInt32(scratch, lootId);
            server.Broadcast(LobbyProtocol.ChLoot, scratch[..len]);
         }

         // Ch 2 (ReliableSequenced): Rapidly updated Boss HP (1000 -> 0); superseded states skipped, 0 guaranteed!
         var bossHp = Math.Max(0, 1000 - ((tick + 1) * 1000 / totalTicks));
         var hpLen = LobbyProtocol.WriteInt32(scratch, bossHp);
         server.Broadcast(LobbyProtocol.ChBossHp, scratch[..hpLen]);

         // Ch 3 (UnreliableSequenced): 30 Hz positions; clients adapt input cadence with RecommendedSendScale
         var posLen = LobbyProtocol.WritePosition(scratch, (uint)tick, MathF.Cos(tick * 0.2f) * 100f, MathF.Sin(tick * 0.2f) * 100f);
         server.Broadcast(LobbyProtocol.ChPosition, scratch[..posLen]);

         for (var i = 0; i < clients.Length; i++)
         {
            var sendScale = clientPeers[i].Stats.RecommendedSendScale;
            var sendEveryNTicks = sendScale < 0.5f ? 3 : (sendScale < 1.0f ? 2 : 1);
            if (tick % sendEveryNTicks == 0)
            {
               clientPeers[i].TrySend(LobbyProtocol.ChPosition, scratch[..posLen]);
            }
         }

         // Ch 4 (UnreliableUnordered): Ephemeral spark/audio cues
         var fxLen = LobbyProtocol.WriteInt32(scratch, tick);
         server.Broadcast(LobbyProtocol.ChFx, scratch[..fxLen]);

         StepNetwork(TimeSpan.FromMilliseconds(33));
         DrainServer();
         DrainClients(expectedMapChecksum);
      }

      // Tail drain window so reliable retransmissions finish under 12% chaos loss
      for (var t = 0; t < 120; t++)
      {
         var allConverged = trackers.All(tr =>
            tr.OrderedPhases.Count == phases.Length
            && tr.UniqueLootIds.Count == expectedLootCount
            && tr.BossHpUpdates.Count > 0 && tr.BossHpUpdates[^1] == 0);
         if (allConverged) break;

         StepNetwork(TimeSpan.FromMilliseconds(25));
         DrainServer();
         DrainClients(expectedMapChecksum);
      }

      Console.WriteLine($"  Server observed Ranger NAT migration: {migrationObserved}");
      Console.WriteLine("  Per-Player Channel Delivery Summary:");
      Console.WriteLine("    Player     | Ch0 Phases | Ch1 Loot | Ch2 BossHP (Updates -> Final) | Ch3 Pos | Ch4 FX | Whisper");
      Console.WriteLine("    -----------+------------+----------+-------------------------------+---------+--------+--------");
      foreach (var tr in trackers)
      {
         var finalHp = tr.BossHpUpdates.Count > 0 ? tr.BossHpUpdates[^1] : -1;
         var whisper = tr.SquadWhispers.Count > 0 ? "Yes" : "No (excluded)";
         Console.WriteLine(
            $"    {tr.Name,-10} | {tr.OrderedPhases.Count}/{phases.Length,-8} | {tr.UniqueLootIds.Count}/{expectedLootCount,-5} | " +
            $"{tr.BossHpUpdates.Count,3} updates -> final HP={finalHp,-4}   | {tr.PositionTicks.Count,7} | {tr.FxReceived,6} | {whisper}");
      }

      // Stage 4: Live Link Quality, Clock Sync & Metrics Summary
      Console.WriteLine("\n[Stage 4] Per-Peer Link Telemetry, Congestion State & Clock Sync");
      for (var i = 0; i < clients.Length; i++)
      {
         var st = clientPeers[i].Stats;
         var srvRetx = serverPeersByName[playerNames[i]].Stats.MessagesRetransmitted;
         var offsetMs = TimeSpan.FromTicks(st.EstimatedClockOffsetTicks).TotalMilliseconds;
         Console.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"  {playerNames[i],-10} | RTT: {st.SmoothedRttMs,5:F1}ms | Loss: {st.PacketLossRate * 100,4:F1}% | " +
            $"State: {st.CongestionState,-9} (scale {st.RecommendedSendScale:F2}x) | ClockOffset: {offsetMs,+6:F0}ms | ServerRetx: {srvRetx}"));
      }

      Console.WriteLine(
         $"  Host Metrics -> Datagrams TX: {totalDatagramsSent}, RX: {totalDatagramsRecv}, " +
         $"Retransmitted Msgs: {totalRetransmits}, AEAD Corruption Rejections: {totalSecurityRejects}");

      // Graceful disconnect all clients
      foreach (var peer in clientPeers) peer.Disconnect("match finished"u8);
      for (var i = 0; i < 8; i++)
      {
         StepNetwork(TimeSpan.FromMilliseconds(20));
         DrainServer();
         DrainClients(expectedMapChecksum);
      }

      foreach (var c in clients) await c.DisposeAsync();

      void StepNetwork(TimeSpan dt)
      {
         server.Tick();
         foreach (var c in clients) c.Tick();
         bannedClient.Tick();

         clientClock.Advance(dt);
         serverClock.Advance(dt);
         hub.DeliverDue();
      }

      void DrainServer()
      {
         server.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.PeerMigrated)
            {
               migrationObserved = true;
               Console.WriteLine($"  [Server] Peer #{evt.Peer.PeerId} ({evt.Peer.Tag}) migrated: {evt.PreviousEndPoint} -> {evt.NewEndPoint}");
            }
         });
      }

      void DrainClients(ulong expectedChecksum)
      {
         for (var i = 0; i < clients.Length; i++)
         {
            var tr = trackers[i];
            clients[i].PollEvents(evt =>
            {
               if (evt.Type != DatagramEventType.MessageReceived) return;
               var span = evt.Payload.Span;

               switch (evt.ChannelId)
               {
                  case LobbyProtocol.ChLifecycle when span.Length >= 1:
                     if (span[0] == LobbyProtocol.OpMapChunk)
                     {
                        tr.MapVerified = LobbyProtocol.ComputeFnv1A64(span[1..]) == expectedChecksum;
                     }
                     else if (span[0] == LobbyProtocol.OpPhaseText)
                     {
                        tr.OrderedPhases.Add(Encoding.UTF8.GetString(span[1..]));
                     }
                     else if (span[0] == LobbyProtocol.OpSquadWhisper)
                     {
                        tr.SquadWhispers.Add(Encoding.UTF8.GetString(span[1..]));
                     }
                     break;

                  case LobbyProtocol.ChLoot when span.Length >= 4:
                     tr.UniqueLootIds.Add(BinaryPrimitives.ReadInt32LittleEndian(span));
                     break;

                  case LobbyProtocol.ChBossHp when span.Length >= 4:
                     tr.BossHpUpdates.Add(BinaryPrimitives.ReadInt32LittleEndian(span));
                     break;

                  case LobbyProtocol.ChPosition when span.Length >= 12:
                     tr.PositionTicks.Add(BinaryPrimitives.ReadUInt32LittleEndian(span[0..4]));
                     break;

                  case LobbyProtocol.ChFx:
                     tr.FxReceived++;
                     break;
               }
            });
         }
      }
   }

   /// <summary>
   /// Stage 5: Real UDP sockets + Async dispatch mode + Single-socket NAT Hole Punching via DatagramRendezvousServer.
   /// </summary>
   private static async Task RunRealUdpNatHolePunchDemoAsync()
   {
      Console.WriteLine("\n[Stage 5] Single-Socket NAT Hole Punching (Real Loopback UDP + DispatchMode.Async)");

      var rdvOptions = new DatagramOptions { Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Crc32C } };
      await using var rendezvous = new DatagramRendezvousServer(new IPEndPoint(IPAddress.Loopback, 0), options: rdvOptions);
      rendezvous.Start();

      await using var hostA = new DatagramHost(
         new IPEndPoint(IPAddress.Loopback, 0),
         LobbyProtocol.CreateMatchOptions(manualTick: false, DatagramEventDispatchMode.Async));
      await using var hostB = new DatagramHost(
         new IPEndPoint(IPAddress.Loopback, 0),
         LobbyProtocol.CreateMatchOptions(manualTick: false, DatagramEventDispatchMode.Async));

      var introATcs = new TaskCompletionSource<DatagramPunchIntroduction>(TaskCreationOptions.RunContinuationsAsynchronously);
      var introBTcs = new TaskCompletionSource<DatagramPunchIntroduction>(TaskCreationOptions.RunContinuationsAsynchronously);
      var p2pMessageTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

      // Register Events callbacks BEFORE Start() in Async dispatch mode
      hostA.Events.PunchIntroduced = (intro, _) => { introATcs.TrySetResult(intro); return ValueTask.CompletedTask; };
      hostB.Events.PunchIntroduced = (intro, _) => { introBTcs.TrySetResult(intro); return ValueTask.CompletedTask; };
      hostB.Events.MessageReceived = (_, ch, mode, payload, _) =>
      {
         p2pMessageTcs.TrySetResult($"[Ch {ch} / {mode}] {Encoding.UTF8.GetString(payload.Span)}");
         return ValueTask.CompletedTask;
      };

      hostA.Start();
      hostB.Start();

      var idA = "squad-valkyrie"u8.ToArray();
      var idB = "squad-ranger"u8.ToArray();

      var mappedA = await hostA.DiscoverPublicEndPointAsync(rendezvous.LocalEndPoint!, idA);
      var mappedB = await hostB.DiscoverPublicEndPointAsync(rendezvous.LocalEndPoint!, idB);
      Console.WriteLine($"  Rendezvous observed Valkyrie={mappedA}, Ranger={mappedB}");

      const ulong punchToken = 0xBEEF_CAFE_2026_0001UL;
      hostA.RequestIntroduction(rendezvous.LocalEndPoint!, idA, idB, punchToken);

      var introA = await introATcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
      var introB = await introBTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));

      // Both peers punch simultaneously on their bound sockets; Valkyrie immediately runs AEAD ConnectAsync over the pinhole
      var punchBTask = hostB.PunchAsync(introB.PeerPublicEndPoint, introB.PeerLanEndPoint, introB.PunchToken);
      var connectATask = hostA.PunchAndConnectAsync(introA.PeerPublicEndPoint, introA.PeerLanEndPoint, introA.PunchToken, "Valkyrie-P2P"u8.ToArray());

      var punchBResult = await punchBTask;
      var p2pPeer = await connectATask;

      Console.WriteLine(string.Create(
         CultureInfo.InvariantCulture,
         $"  Direct P2P punch succeeded={punchBResult.Succeeded}, RTT={punchBResult.RoundTripTime.TotalMilliseconds:F2}ms, PeerId={p2pPeer?.PeerId}"));

      p2pPeer!.TrySend(LobbyProtocol.ChLifecycle, "Direct encrypted P2P hello after NAT punch!"u8);
      var received = await p2pMessageTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
      Console.WriteLine($"  Ranger received direct P2P message: {received}");
   }
}
