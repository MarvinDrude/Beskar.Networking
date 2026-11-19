using System.Buffers.Binary;
using System.Diagnostics.Metrics;
using System.Net;
using System.Text;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Protocol;
using Beskar.Networking.Datagram.Telemetry;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Networking.Datagram.Tests;

public class ProductionStressAndEdgeCaseTests
{
   private static DatagramOptions CreateStandardOptions(
      DatagramSecurityMode security = DatagramSecurityMode.Aead,
      int windowSize = 256,
      int maxQueue = 1024,
      int maxFragCount = 160,
      DatagramEventDispatchMode dispatchMode = DatagramEventDispatchMode.Poll,
      bool manualTick = true)
   {
      return new DatagramOptions
      {
         ManualTick = manualTick,
         DispatchMode = dispatchMode,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero,
         PeerTimeout = TimeSpan.FromSeconds(10),
         MaxReliableRetryDuration = TimeSpan.FromSeconds(15),
         KeepAliveInterval = TimeSpan.FromMilliseconds(200),
         Security = new DatagramSecurityOptions { Mode = security }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered, priority: 255, windowSize: windowSize, maxSendQueueDepth: maxQueue, allowFragmentation: true, maxFragmentCount: maxFragCount)
      .AddChannel(1, DatagramDeliveryMode.ReliableUnordered, priority: 220, windowSize: windowSize, maxSendQueueDepth: maxQueue, allowFragmentation: true, maxFragmentCount: maxFragCount)
      .AddChannel(2, DatagramDeliveryMode.UnreliableSequenced, priority: 150, allowFragmentation: false)
      .AddChannel(3, DatagramDeliveryMode.UnreliableUnordered, priority: 100, allowFragmentation: false)
      .AddChannel(4, DatagramDeliveryMode.ReliableSequenced, priority: 180, windowSize: windowSize, maxSendQueueDepth: maxQueue, allowFragmentation: true, maxFragmentCount: maxFragCount);
   }

   private static async Task<(DatagramHost Server, DatagramHost Client, DatagramPeer ClientPeer, DatagramPeer ServerPeer)>
      ConnectPairAsync(SimulatedNetworkHub hub, DatagramOptions? serverOpts = null, DatagramOptions? clientOpts = null, int serverPort = 9900)
   {
      var serverEp = new IPEndPoint(IPAddress.Loopback, serverPort);
      var server = new DatagramHost(serverEp, serverOpts ?? CreateStandardOptions(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), clientOpts ?? CreateStandardOptions(), hub.CreateTransport(), hub.Time);
      server.Start();
      client.Start();

      var t = client.ConnectAsync(serverEp);
      for (var i = 0; i < 60 && !t.IsCompleted; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }

      var cp = await t;
      DatagramPeer? sp = null;
      server.PollEvents(evt => { if (evt.Type == DatagramEventType.PeerConnected) sp = evt.Peer; });
      client.PollEvents(_ => { });

      await Assert.That(sp).IsNotNull();
      return (server, client, cp, sp!);
   }

   [Test]
   public async Task SentPacketLedgerAndAckTracker_MultiWrapPast2500PhysicalDatagramsUnder20PercentLoss()
   {
      var hub = new SimulatedNetworkHub(seed: 1001, chaos: new SimulatedChaosProfile
      {
         PacketDropRate = 0.20,
         PacketDuplicateRate = 0.10,
         ReorderRate = 0.25,
         MinLatency = TimeSpan.FromMilliseconds(5),
         MaxLatency = TimeSpan.FromMilliseconds(35),
         ReorderExtraDelay = TimeSpan.FromMilliseconds(30)
      });

      var (server, client, clientPeer, _) = await ConnectPairAsync(hub, serverPort: 9901);

      const int totalMessages = 2600; // 1 message per tick => >2,600 physical datagrams (>2.5 full 1024-slot ledger wraps!)
      var received = new List<int>(totalMessages);
      var buf = new byte[4];

      for (var i = 0; i < totalMessages; i++)
      {
         BinaryPrimitives.WriteInt32LittleEndian(buf, i);
         while (clientPeer.TrySend(0, buf) == DatagramSendResult.QueueFull)
         {
            client.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
            server.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
            Drain();
         }

         // Tick once per message so each message goes in its own physical UDP datagram
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(10));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(10));
         Drain();
      }

      for (var t = 0; t < 400 && received.Count < totalMessages; t++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         Drain();
      }

      await Assert.That(clientPeer.Stats.DatagramsSent).IsGreaterThan(2500);
      await Assert.That(received.Count).IsEqualTo(totalMessages);
      for (var i = 0; i < totalMessages; i++)
      {
         await Assert.That(received[i]).IsEqualTo(i);
      }

      await client.DisposeAsync();
      await server.DisposeAsync();

      void Drain()
      {
         server.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 0)
               received.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
         });
         client.PollEvents(_ => { });
      }
   }

   [Test]
   public async Task HeterogeneousEightPlayerListenServer_IsolatesSlowLossyMigratingAndReconnectingPlayers()
   {
      var hub = new SimulatedNetworkHub(seed: 1002);
      var hostEp = new IPEndPoint(IPAddress.Loopback, 9902);

      var hostOpts = CreateStandardOptions();
      hostOpts.PeerTimeout = TimeSpan.FromSeconds(2);
      var host = new DatagramHost(hostEp, hostOpts, hub.CreateTransport(), hub.Time);
      host.Events.ValidateConnection = ctx =>
      {
         ctx.PeerTag = Encoding.UTF8.GetString(ctx.ConnectMetadata.Span);
         return ValueTask.CompletedTask;
      };
      host.Start();

      // 7 remote clients + 1 listen-server host = 8-player party lobby!
      const int remoteCount = 7;
      var transports = new SimulatedDatagramTransport[remoteCount];
      var clients = new DatagramHost[remoteCount];
      var peers = new DatagramPeer[remoteCount];
      var names = new[] { "P1-LAN", "P2-LAN", "P3-WiFiLoss", "P4-WiFiLoss", "P5-MobileMig", "P6-Reconnect", "P7-DropOut" };

      for (var i = 0; i < remoteCount; i++)
      {
         transports[i] = hub.CreateTransport();
         clients[i] = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateStandardOptions(), transports[i], hub.Time);
         clients[i].Start();
      }

      var connectTasks = new Task<DatagramPeer>[remoteCount];
      for (var i = 0; i < remoteCount; i++)
      {
         connectTasks[i] = clients[i].ConnectAsync(hostEp, Encoding.UTF8.GetBytes(names[i]));
      }

      for (var tick = 0; tick < 50 && connectTasks.Any(t => !t.IsCompleted); tick++)
      {
         for (var i = 0; i < remoteCount; i++) clients[i].Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         host.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }

      for (var i = 0; i < remoteCount; i++) peers[i] = await connectTasks[i];
      await Assert.That(host.PeerCount).IsEqualTo(remoteCount);
      host.PollEvents(_ => { });
      foreach (var c in clients) c.PollEvents(_ => { });

      // Per-player heterogeneous network conditions:
      // P3 & P4 experience 25% packet loss; P7 goes completely dark!
      var lossRng = new Random(777);
      var p3Ep = clients[2].LocalEndPoint!;
      var p4Ep = clients[3].LocalEndPoint!;
      var p7Ep = clients[6].LocalEndPoint!;

      hub.OnPacketSending = (src, dst, _) =>
      {
         if (src.Equals(p7Ep) || dst.Equals(p7Ep)) return false; // P7 dead link
         if ((src.Equals(p3Ep) || dst.Equals(p3Ep) || src.Equals(p4Ep) || dst.Equals(p4Ep)) && lossRng.NextDouble() < 0.25)
            return false;
         return true;
      };

      var turnOrderPerClient = Enumerable.Range(0, remoteCount).Select(_ => new List<int>()).ToArray();
      var pickupsPerClient = Enumerable.Range(0, remoteCount).Select(_ => new HashSet<int>()).ToArray();
      var snapshotsPerClient = Enumerable.Range(0, remoteCount).Select(_ => new List<int>()).ToArray();
      var latestHudPerClient = new int[remoteCount];
      var hostInputsFromPlayers = new Dictionary<string, int>();
      var hostSawMigration = false;
      var hostTimedOutPlayers = new List<string>();

      const int matchSteps = 80;
      var p6Reconnected = false;
      Task<DatagramPeer>? p6ReconnectTask = null;

      for (var step = 0; step < matchSteps; step++)
      {
         // Mid-match events!
         if (step == 25)
         {
            // P5 migrates cellular IP/port mid-match
            hub.MigrateEndpoint(transports[4], new IPEndPoint(IPAddress.Parse("10.55.0.5"), 55005));
         }

         if (step == 30)
         {
            // P6 gracefully disconnects mid-match
            peers[5].Disconnect("restarting"u8);
         }

         if (step == 42 && !p6Reconnected)
         {
            // P6 reconnects!
            p6ReconnectTask = clients[5].ConnectAsync(hostEp, Encoding.UTF8.GetBytes("P6-Reconnect"));
            p6Reconnected = true;
         }

         if (p6ReconnectTask is not null && p6ReconnectTask.IsCompletedSuccessfully)
         {
            peers[5] = await p6ReconnectTask;
            p6ReconnectTask = null;
         }

         // Host broadcasts mixed-channel match traffic
         var stepBytes = BitConverter.GetBytes(step);
         host.Broadcast(0, stepBytes); // ReliableOrdered turn/match event
         host.Broadcast(1, stepBytes); // ReliableUnordered pickup event
         host.Broadcast(2, stepBytes); // UnreliableSequenced snapshot
         host.Broadcast(4, stepBytes); // ReliableSequenced HUD summary

         // Active clients send inputs to host
         for (var i = 0; i < 6; i++)
         {
            if (peers[i].State == DatagramPeerState.Connected)
            {
               peers[i].TrySend(2, stepBytes);
            }
         }

         host.Tick();
         for (var i = 0; i < 6; i++) clients[i].Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(35));
         host.Tick();
         for (var i = 0; i < 6; i++) clients[i].Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(35));

         DrainAll();
      }

      // Final flush window for lossy P3/P4 to finish retransmits
      for (var tick = 0; tick < 120; tick++)
      {
         host.Tick();
         for (var i = 0; i < 6; i++) clients[i].Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(25));
         DrainAll();
      }

      // P1, P2 (LAN), P3, P4 (25% loss Wi-Fi), and P5 (NAT-migrated) ALL received 100% of ReliableOrdered, ReliableUnordered, and converged HUD!
      for (var i = 0; i < 5; i++)
      {
         await Assert.That(turnOrderPerClient[i].Count).IsEqualTo(matchSteps);
         for (var s = 0; s < matchSteps; s++)
         {
            await Assert.That(turnOrderPerClient[i][s]).IsEqualTo(s);
         }

         await Assert.That(pickupsPerClient[i].Count).IsEqualTo(matchSteps);
         await Assert.That(latestHudPerClient[i]).IsEqualTo(matchSteps - 1);

         // UnreliableSequenced snapshots are strictly increasing
         await Assert.That(snapshotsPerClient[i].Count).IsGreaterThan(0);
         for (var k = 1; k < snapshotsPerClient[i].Count; k++)
         {
            await Assert.That(snapshotsPerClient[i][k]).IsGreaterThan(snapshotsPerClient[i][k - 1]);
         }
      }

      await Assert.That(hostSawMigration).IsTrue();
      await Assert.That(hostTimedOutPlayers).Contains("P7-DropOut");
      await Assert.That(peers[5].State).IsEqualTo(DatagramPeerState.Connected); // P6 reconnected cleanly
      await Assert.That(host.PeerCount).IsEqualTo(6); // P1..P6 alive, P7 timed out!

      foreach (var c in clients) await c.DisposeAsync();
      await host.DisposeAsync();

      void DrainAll()
      {
         host.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.PeerMigrated) hostSawMigration = true;
            if (evt.Type == DatagramEventType.PeerDisconnected && evt.DisconnectReason == DatagramDisconnectReason.Timeout)
               hostTimedOutPlayers.Add((string)evt.Peer.Tag!);
            if (evt.Type == DatagramEventType.MessageReceived)
            {
               var name = (string)evt.Peer.Tag!;
               hostInputsFromPlayers[name] = hostInputsFromPlayers.GetValueOrDefault(name) + 1;
            }
         });

         for (var i = 0; i < 6; i++)
         {
            var idx = i;
            clients[idx].PollEvents(evt =>
            {
               if (evt.Type != DatagramEventType.MessageReceived) return;
               var v = BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span);
               switch (evt.ChannelId)
               {
                  case 0: turnOrderPerClient[idx].Add(v); break;
                  case 1: pickupsPerClient[idx].Add(v); break;
                  case 2: snapshotsPerClient[idx].Add(v); break;
                  case 4: latestHudPerClient[idx] = v; break;
               }
            });
         }
      }
   }

   [Test]
   public async Task DisconnectAndImmediateReconnect_FromIdenticalEndPoint_IsolatesOldSessionTokens()
   {
      var hub = new SimulatedNetworkHub(seed: 1003);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9903);
      var fixedClientEp = new IPEndPoint(IPAddress.Loopback, 54100);

      var server = new DatagramHost(serverEp, CreateStandardOptions(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(fixedClientEp, CreateStandardOptions(), hub.CreateTransport(), hub.Time);
      server.Start();
      client.Start();

      // Session 1
      var t1 = client.ConnectAsync(serverEp, "session-1"u8.ToArray());
      for (var i = 0; i < 20 && !t1.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var peer1 = await t1;
      var oldToken = peer1.SessionToken;
      server.PollEvents(_ => { });

      // Capture a genuine Session 1 packet
      byte[]? staleSession1Pkt = null;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (src.Equals(fixedClientEp) && DatagramWireHeader.TryRead(raw, out var hdr) && hdr.Kind == DatagramPacketKind.Data)
            staleSession1Pkt = (byte[])raw.Clone();
         return true;
      };
      peer1.TrySend(0, "msg-s1"u8);
      client.Tick();
      server.Tick();
      hub.OnPacketSending = null;
      server.PollEvents(_ => { });
      await Assert.That(staleSession1Pkt).IsNotNull();

      // Disconnect gracefully
      peer1.Disconnect("bye"u8);
      for (var i = 0; i < 10; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.PollEvents(_ => { });
         client.PollEvents(_ => { });
      }
      await Assert.That(server.PeerCount).IsEqualTo(0);

      // Immediately reconnect from the EXACT SAME fixedClientEp!
      var t2 = client.ConnectAsync(serverEp, "session-2"u8.ToArray());
      for (var i = 0; i < 20 && !t2.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var peer2 = await t2;
      await Assert.That(peer2.SessionToken).IsNotEqualTo(oldToken);
      await Assert.That(server.PeerCount).IsEqualTo(1);
      server.PollEvents(_ => { });

      // Inject stray old Session 1 packet from exact same IP:Port -> MUST be ignored!
      hub.InjectRawPacket(fixedClientEp, serverEp, staleSession1Pkt!);
      peer2.TrySend(0, "msg-s2"u8);
      client.Tick();
      server.Tick();

      var got = new List<string>();
      server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived)
            got.Add(Encoding.UTF8.GetString(evt.Payload.Span));
      });

      await Assert.That(got).IsEquivalentTo(["msg-s2"]);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task AdversarialFuzzing_1500MalformedPackets_NeverCrashesOrDisruptsLivePeers()
   {
      var hub = new SimulatedNetworkHub(seed: 1004);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9904);

      var server = new DatagramHost(serverEp, CreateStandardOptions(), hub.CreateTransport(), hub.Time);
      var c1 = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateStandardOptions(), hub.CreateTransport(), hub.Time);
      var c2 = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateStandardOptions(), hub.CreateTransport(), hub.Time);
      server.Start(); c1.Start(); c2.Start();

      var t1 = c1.ConnectAsync(serverEp);
      var t2 = c2.ConnectAsync(serverEp);
      for (var i = 0; i < 25 && (!t1.IsCompleted || !t2.IsCompleted); i++)
      {
         c1.Tick(); c2.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var p1 = await t1;
      var p2 = await t2;
      server.PollEvents(_ => { });

      var rng = new Random(1337);
      var fuzzIp = new IPEndPoint(IPAddress.Parse("203.0.113.55"), 44444);

      // Blast 1,500 diverse adversarial / malformed / truncated / off-curve packets!
      for (var i = 0; i < 1500; i++)
      {
         byte[] evil;
         var kind = i % 10;
         switch (kind)
         {
            case 0:
               evil = []; // 0-byte packet
               break;
            case 1:
               evil = new byte[rng.Next(1, DatagramWireHeader.Size)]; // truncated header
               rng.NextBytes(evil);
               break;
            case 2:
               // Valid Data header with active peer's SessionToken + truncated AEAD body (<16B tag)
               evil = new byte[DatagramWireHeader.Size + rng.Next(0, 15)];
               new DatagramWireHeader(DatagramPacketKind.Data, 1, true, p1.SessionToken, (uint)(500 + i), 0, 0).Write(evil);
               break;
            case 3:
               // Valid Data header with active SessionToken + IsEncrypted=false downgrade attempt!
               evil = new byte[DatagramWireHeader.Size + 32];
               new DatagramWireHeader(DatagramPacketKind.Data, 1, false, p1.SessionToken, (uint)(500 + i), 0, 0).Write(evil);
               break;
            case 4:
               // ConnectRequest with off-curve EC P-256 point (all 0xFF)
               evil = new byte[120];
               new DatagramWireHeader(DatagramPacketKind.ConnectRequest, 1, false, 0, 0, 0, 0).Write(evil);
               Array.Fill(evil, (byte)0xFF, DatagramWireHeader.Size, 95);
               break;
            case 5:
               // ConnectAccept with bogus cookie & all-zero EC point
               evil = new byte[220];
               new DatagramWireHeader(DatagramPacketKind.ConnectAccept, 1, true, 123, 1, 0, 0).Write(evil);
               rng.NextBytes(evil.AsSpan(DatagramWireHeader.Size));
               break;
            case 6:
               // Oversized ConnectAck (> 2048 bytes) aimed at c1
               evil = new byte[3000];
               new DatagramWireHeader(DatagramPacketKind.ConnectAck, 1, true, p1.SessionToken, 1, 0, 0).Write(evil);
               hub.InjectRawPacket(serverEp, c1.LocalEndPoint!, evil);
               continue;
            case 7:
               // Truncated TimeSyncPing / Pong
               evil = new byte[DatagramWireHeader.Size + 3];
               new DatagramWireHeader(DatagramPacketKind.TimeSyncPing, 1, true, p2.SessionToken, (uint)(600 + i), 0, 0).Write(evil);
               break;
            case 8:
               // Corrupted PunchProbe & Rendezvous packets
               evil = new byte[DatagramWireHeader.Size + rng.Next(0, 12)];
               new DatagramWireHeader(DatagramPacketKind.PunchProbe, 1, false, 0, 0, 0, 0).Write(evil);
               break;
            default:
               // Pure random noise 21..1300 bytes
               evil = new byte[rng.Next(21, 1300)];
               rng.NextBytes(evil);
               break;
         }

         hub.InjectRawPacket(i % 2 == 0 ? fuzzIp : c1.LocalEndPoint!, serverEp, evil);

         if (i % 50 == 0)
         {
            // Interleave real legitimate traffic from both peers during fuzz storm!
            p1.TrySend(0, BitConverter.GetBytes(i));
            p2.TrySend(0, BitConverter.GetBytes(i + 10_000));
            c1.Tick(); c2.Tick();
            server.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(10));
         }
      }

      var fromP1 = new List<int>();
      var fromP2 = new List<int>();
      for (var t = 0; t < 40; t++)
      {
         c1.Tick(); c2.Tick();
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.PollEvents(evt =>
         {
            if (evt.Type != DatagramEventType.MessageReceived) return;
            var v = BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span);
            if (v < 10_000) fromP1.Add(v);
            else fromP2.Add(v);
         });
         c1.PollEvents(_ => { });
         c2.PollEvents(_ => { });
      }

      await Assert.That(server.PeerCount).IsEqualTo(2);
      await Assert.That(fromP1.Count).IsEqualTo(30); // 1500 / 50
      await Assert.That(fromP2.Count).IsEqualTo(30);

      await c1.DisposeAsync();
      await c2.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task SingleSocketHybridMesh_ThreeNodesSimultaneousInboundAndOutbound_NoCollisions()
   {
      var hub = new SimulatedNetworkHub(seed: 1005);
      var epA = new IPEndPoint(IPAddress.Loopback, 9910);
      var epB = new IPEndPoint(IPAddress.Loopback, 9911);
      var epC = new IPEndPoint(IPAddress.Loopback, 9912);

      var hostA = new DatagramHost(epA, CreateStandardOptions(), hub.CreateTransport(), hub.Time);
      var hostB = new DatagramHost(epB, CreateStandardOptions(), hub.CreateTransport(), hub.Time);
      var hostC = new DatagramHost(epC, CreateStandardOptions(), hub.CreateTransport(), hub.Time);
      hostA.Start(); hostB.Start(); hostC.Start();

      // Triangle: A -> B, B -> C, C -> A simultaneously on each host's single bound socket!
      var tAB = hostA.ConnectAsync(epB, "from-A"u8.ToArray());
      var tBC = hostB.ConnectAsync(epC, "from-B"u8.ToArray());
      var tCA = hostC.ConnectAsync(epA, "from-C"u8.ToArray());

      for (var i = 0; i < 40 && (!tAB.IsCompleted || !tBC.IsCompleted || !tCA.IsCompleted); i++)
      {
         hostA.Tick(); hostB.Tick(); hostC.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }

      var pAB = await tAB;
      var pBC = await tBC;
      var pCA = await tCA;

      // Every host holds exactly 2 peers (1 inbound + 1 outbound) with unique local PeerIds (1 and 2)!
      await Assert.That(hostA.PeerCount).IsEqualTo(2);
      await Assert.That(hostB.PeerCount).IsEqualTo(2);
      await Assert.That(hostC.PeerCount).IsEqualTo(2);

      // Broadcast from every host -> every other host receives 2 distinct messages!
      hostA.Broadcast(0, "hello-from-A"u8);
      hostB.Broadcast(0, "hello-from-B"u8);
      hostC.Broadcast(0, "hello-from-C"u8);

      var gotA = new HashSet<string>();
      var gotB = new HashSet<string>();
      var gotC = new HashSet<string>();

      for (var i = 0; i < 20; i++)
      {
         hostA.Tick(); hostB.Tick(); hostC.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         hostA.PollEvents(e => { if (e.Type == DatagramEventType.MessageReceived) gotA.Add(Encoding.UTF8.GetString(e.Payload.Span)); });
         hostB.PollEvents(e => { if (e.Type == DatagramEventType.MessageReceived) gotB.Add(Encoding.UTF8.GetString(e.Payload.Span)); });
         hostC.PollEvents(e => { if (e.Type == DatagramEventType.MessageReceived) gotC.Add(Encoding.UTF8.GetString(e.Payload.Span)); });
      }

      await Assert.That(gotA).IsEquivalentTo(["hello-from-B", "hello-from-C"]);
      await Assert.That(gotB).IsEquivalentTo(["hello-from-A", "hello-from-C"]);
      await Assert.That(gotC).IsEquivalentTo(["hello-from-A", "hello-from-B"]);
      await Assert.That(pAB.State).IsEqualTo(DatagramPeerState.Connected);
      await Assert.That(pBC.State).IsEqualTo(DatagramPeerState.Connected);
      await Assert.That(pCA.State).IsEqualTo(DatagramPeerState.Connected);

      await hostA.DisposeAsync();
      await hostB.DisposeAsync();
      await hostC.DisposeAsync();
   }

   [Test]
   public async Task Large100KbStateDump_90FragmentsUnderHeavyLossAndReorder_ReassemblesByteForByte()
   {
      var hub = new SimulatedNetworkHub(seed: 1006, chaos: new SimulatedChaosProfile
      {
         PacketDropRate = 0.20,
         PacketDuplicateRate = 0.10,
         ReorderRate = 0.30,
         MinLatency = TimeSpan.FromMilliseconds(5),
         MaxLatency = TimeSpan.FromMilliseconds(35),
         ReorderExtraDelay = TimeSpan.FromMilliseconds(30)
      });

      var (server, client, clientPeer, _) = await ConnectPairAsync(hub, serverPort: 9906);

      var dump100Kb = new byte[100_000]; // ~87 fragments on 1200B AEAD MTU!
      var dump65Kb = new byte[65_000];   // ~57 fragments!
      new Random(90).NextBytes(dump100Kb);
      new Random(91).NextBytes(dump65Kb);

      await Assert.That(clientPeer.TrySend(0, dump100Kb)).IsEqualTo(DatagramSendResult.Queued);
      await Assert.That(clientPeer.TrySend(1, dump65Kb)).IsEqualTo(DatagramSendResult.Queued);

      byte[]? gotOrdered = null;
      byte[]? gotUnordered = null;

      for (var t = 0; t < 450 && (gotOrdered is null || gotUnordered is null); t++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));

         server.PollEvents(evt =>
         {
            if (evt.Type != DatagramEventType.MessageReceived) return;
            if (evt.ChannelId == 0) gotOrdered = evt.Payload.ToArray();
            if (evt.ChannelId == 1) gotUnordered = evt.Payload.ToArray();
         });
         client.PollEvents(_ => { });
      }

      await Assert.That(gotOrdered).IsEquivalentTo(dump100Kb);
      await Assert.That(gotUnordered).IsEquivalentTo(dump65Kb);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task UnreliableSequenced_WrapAroundPast65535_DropsStalePreWrapPacketAfterPostWrapArrives()
   {
      var hub = new SimulatedNetworkHub(seed: 1007);
      var (server, client, clientPeer, _) = await ConnectPairAsync(
         hub,
         CreateStandardOptions(DatagramSecurityMode.Crc32C),
         CreateStandardOptions(DatagramSecurityMode.Crc32C),
         serverPort: 9907);

      var buf = new byte[4];
      // Advance Channel 2 sequence to 65,533
      for (var i = 0; i < 65_534; i++)
      {
         BinaryPrimitives.WriteInt32LittleEndian(buf, i);
         clientPeer.TrySend(2, buf);
         if (i % 128 == 0)
         {
            client.Tick();
            hub.DeliverDue();
            server.Tick();
            server.PollEvents(_ => { });
            client.PollEvents(_ => { });
         }
      }
      client.Tick(); hub.DeliverDue(); server.Tick();
      server.PollEvents(_ => { });

      // Hold seq 65534 (i=65534), let seq 65535, 0, 1, 2 pass cleanly!
      byte[]? heldPreWrap = null;
      IPEndPoint? clientEp = null;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (src.Equals(client.LocalEndPoint) && DatagramWireHeader.TryRead(raw, out var hdr) && hdr.Kind == DatagramPacketKind.Data)
         {
            heldPreWrap = (byte[])raw.Clone();
            clientEp = src;
            return false;
         }
         return true;
      };

      BinaryPrimitives.WriteInt32LittleEndian(buf, 65_534);
      clientPeer.TrySend(2, buf);
      client.Tick();
      hub.OnPacketSending = null;
      await Assert.That(heldPreWrap).IsNotNull();

      var postWrapReceived = new List<int>();
      for (var i = 65_535; i <= 65_538; i++)
      {
         BinaryPrimitives.WriteInt32LittleEndian(buf, i);
         clientPeer.TrySend(2, buf);
         client.Tick();
         hub.DeliverDue();
         server.Tick();
         server.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 2)
               postWrapReceived.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
         });
      }

      // Now inject delayed pre-wrap packet (65534) AFTER post-wrap 65538 (seq=2) arrived -> MUST BE DROPPED!
      hub.InjectRawPacket(clientEp!, server.LocalEndPoint!, heldPreWrap!);
      server.Tick();
      server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 2)
            postWrapReceived.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
      });

      await Assert.That(postWrapReceived).IsEquivalentTo([65_535, 65_536, 65_537, 65_538]);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   [NotInParallel]
   public async Task ConnectionChurn_20Cycles_AndKeepAliveIdleSurvival_AndMutualDisconnect()
   {
      // 1. Track DatagramMetrics.PeersActive delta across 20 churn cycles
      long activeDelta = 0;
      using var listener = new MeterListener();
      listener.InstrumentPublished = (inst, l) =>
      {
         if (inst.Meter.Name == DatagramMetrics.MeterName && inst.Name == "beskar.datagram.peers.active")
            l.EnableMeasurementEvents(inst);
      };
      listener.SetMeasurementEventCallback<long>((_, value, _, _) => Interlocked.Add(ref activeDelta, value));
      listener.Start();

      var hub = new SimulatedNetworkHub(seed: 1008);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9908);
      var serverOpts = CreateStandardOptions();
      serverOpts.PeerTimeout = TimeSpan.FromMilliseconds(500);
      serverOpts.KeepAliveInterval = TimeSpan.FromMilliseconds(100);

      var server = new DatagramHost(serverEp, serverOpts, hub.CreateTransport(), hub.Time);
      server.Start();

      var bigChunk = new byte[4000];
      for (var cycle = 0; cycle < 20; cycle++)
      {
         var cOpts = CreateStandardOptions();
         cOpts.PeerTimeout = TimeSpan.FromMilliseconds(500);
         cOpts.KeepAliveInterval = TimeSpan.FromMilliseconds(100);
         var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), cOpts, hub.CreateTransport(), hub.Time);
         client.Start();

         var ct = client.ConnectAsync(serverEp);
         for (var i = 0; i < 20 && !ct.IsCompleted; i++)
         {
            client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
            server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         }
         var peer = await ct;

         // Send fragmented + small traffic
         peer.TrySend(0, bigChunk);
         peer.TrySend(1, BitConverter.GetBytes(cycle));
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(10));
         server.Tick();

         if (cycle % 3 == 0)
         {
            // Mutual simultaneous disconnect!
            peer.Disconnect("c-bye"u8);
            foreach (var sp in server.Peers) sp.Disconnect("s-bye"u8);
            client.Tick(); server.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            client.Tick(); server.Tick();
         }
         else if (cycle % 3 == 1)
         {
            // Abrupt client dispose mid-flight -> server times out cleanly
            await client.DisposeAsync();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(600));
            server.Tick();
         }
         else
         {
            // Graceful one-sided disconnect
            peer.Disconnect();
            for (var i = 0; i < 5; i++)
            {
               client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
               server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
            }
         }

         server.PollEvents(_ => { });
         client.PollEvents(_ => { });
         await client.DisposeAsync();
      }

      await Assert.That(server.PeerCount).IsEqualTo(0);

      // 2. KeepAlive survival: stay idle for 2.5 seconds (5x PeerTimeout = 500ms!) with ZERO app sends
      var idleOpts = CreateStandardOptions();
      idleOpts.PeerTimeout = TimeSpan.FromMilliseconds(500);
      idleOpts.KeepAliveInterval = TimeSpan.FromMilliseconds(100);
      var idleClient = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), idleOpts, hub.CreateTransport(), hub.Time);
      idleClient.Start();

      var connectIdle = idleClient.ConnectAsync(serverEp);
      for (var i = 0; i < 20 && !connectIdle.IsCompleted; i++)
      {
         idleClient.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
      }
      var idlePeer = await connectIdle;

      // Duplicate in-progress handshake guard check on a fresh client
      var dupClient = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), idleOpts, hub.CreateTransport(), hub.Time);
      dupClient.Start();
      var firstConnect = dupClient.ConnectAsync(serverEp);
      await Assert.That(async () => { await dupClient.ConnectAsync(serverEp); }).Throws<InvalidOperationException>();
      await dupClient.DisposeAsync();
      try { await firstConnect; } catch (OperationCanceledException) { }

      // Pump 2.5 seconds of pure silence (50 ticks x 50ms)
      for (var t = 0; t < 50; t++)
      {
         idleClient.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(25));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(25));
         server.PollEvents(_ => { });
         idleClient.PollEvents(_ => { });
      }

      await Assert.That(idlePeer.State).IsEqualTo(DatagramPeerState.Connected);
      await Assert.That(server.PeerCount).IsEqualTo(1);

      idlePeer.TrySend(0, "still-alive!"u8);
      string? aliveMsg = null;
      for (var t = 0; t < 10; t++)
      {
         idleClient.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         server.PollEvents(evt => { if (evt.Type == DatagramEventType.MessageReceived) aliveMsg = Encoding.UTF8.GetString(evt.Payload.Span); });
      }
      await Assert.That(aliveMsg).IsEqualTo("still-alive!");

      await idleClient.DisposeAsync();
      await server.DisposeAsync();

      await Assert.That(activeDelta).IsEqualTo(0L);
   }
}
