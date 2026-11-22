using System.Collections.Concurrent;
using System.Net;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Nat;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Protocol;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Networking.Datagram.Tests;

public class DeliveryTrackingRendezvousAndPackingTests
{
   private static DatagramOptions CreateOptions(
      DatagramSecurityMode security = DatagramSecurityMode.Crc32C,
      int maxDatagramsPerTick = 32,
      bool allowUnreliableFrag = false)
   {
      return new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxBytesPerSecondPerPeer = null,
         MaxDatagramsPerTick = maxDatagramsPerTick,
         TimeSyncInterval = TimeSpan.Zero,
         ConnectTimeout = TimeSpan.FromSeconds(5),
         HandshakeRetryInterval = TimeSpan.FromMilliseconds(100),
         PeerTimeout = TimeSpan.FromSeconds(15),
         MaxReliableRetryDuration = TimeSpan.FromSeconds(15),
         Security = new DatagramSecurityOptions { Mode = security }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered, priority: 255, allowFragmentation: true)
      .AddChannel(1, DatagramDeliveryMode.ReliableUnordered, priority: 220, allowFragmentation: true)
      .AddChannel(2, DatagramDeliveryMode.ReliableSequenced, priority: 200, allowFragmentation: true)
      .AddChannel(3, DatagramDeliveryMode.UnreliableSequenced, priority: 150, allowFragmentation: allowUnreliableFrag)
      .AddChannel(4, DatagramDeliveryMode.UnreliableUnordered, priority: 100, allowFragmentation: allowUnreliableFrag);
   }

   private static async Task<(DatagramHost Server, DatagramHost Client, DatagramPeer ClientPeer, DatagramPeer ServerPeer)>
      ConnectPairAsync(SimulatedNetworkHub hub, DatagramOptions? serverOpts = null, DatagramOptions? clientOpts = null, int port = 10300)
   {
      var serverEp = new IPEndPoint(IPAddress.Loopback, port);
      var server = new DatagramHost(serverEp, serverOpts ?? CreateOptions(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), clientOpts ?? CreateOptions(), hub.CreateTransport(), hub.Time);
      server.Start();
      client.Start();

      var ct = client.ConnectAsync(serverEp);
      for (var i = 0; i < 40 && !ct.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
      }

      var cp = await ct;
      DatagramPeer? sp = null;
      server.PollEvents(evt => { if (evt.Type == DatagramEventType.PeerConnected) sp = evt.Peer; });
      client.PollEvents(_ => { });
      await Assert.That(sp).IsNotNull();
      return (server, client, cp, sp!);
   }

   [Test]
   public async Task UnreliableDeliveryNotification_ReportsExactAckedAndLostTokens_IncludingZeroToken()
   {
      var hub = new SimulatedNetworkHub(seed: 6001);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub, port: 10301);

      // Send 10 snapshots (tokens: 0, 101..109) in 10 separate datagrams; drop #3 (token 102) and #6 (token 105)!
      var dataSeq = 0;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (!src.Equals(client.LocalEndPoint)) return true;
         if (!DatagramWireHeader.TryRead(raw, out var hdr) || hdr.Kind != DatagramPacketKind.Data) return true;
         dataSeq++;
         return dataSeq is not (3 or 6);
      };

      var tokens = new ulong[] { 0, 101, 102, 103, 104, 105, 106, 107, 108, 109 };
      var notifications = new List<(ulong Token, DatagramDeliveryStatus Status)>();

      for (var i = 0; i < tokens.Length; i++)
      {
         await Assert.That(clientPeer.TrySend(3, BitConverter.GetBytes(tokens[i]), deliveryToken: tokens[i]))
            .IsEqualTo(DatagramSendResult.Queued);

         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         Drain();
      }

      hub.OnPacketSending = null;

      for (var t = 0; t < 30 && notifications.Count < tokens.Length; t++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(30));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(30));
         Drain();
      }

      // Every token resolved EXACTLY once (no duplicates, no missing tokens)
      await Assert.That(notifications.Count).IsEqualTo(tokens.Length);
      var byToken = notifications.ToDictionary(n => n.Token, n => n.Status);

      foreach (var tok in tokens)
      {
         var expected = tok is 102 or 105 ? DatagramDeliveryStatus.Lost : DatagramDeliveryStatus.Acknowledged;
         await Assert.That(byToken[tok]).IsEqualTo(expected);
      }

      await client.DisposeAsync();
      await server.DisposeAsync();

      void Drain()
      {
         server.PollEvents(_ => { });
         client.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.DeliveryNotification)
            {
               notifications.Add((evt.DeliveryToken, evt.DeliveryStatus));
            }
         });
      }
   }

   [Test]
   public async Task UnreliableDeliveryNotification_DroppedTailPacketWithNoSubsequentTraffic_NotifiesLostOnTimeout()
   {
      var hub = new SimulatedNetworkHub(seed: 6002);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub, port: 10302);

      // Drop ONLY the single tail Data packet
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (src.Equals(client.LocalEndPoint)
             && DatagramWireHeader.TryRead(raw, out var hdr)
             && hdr.Kind == DatagramPacketKind.Data)
         {
            return false;
         }
         return true;
      };

      await Assert.That(clientPeer.TrySend(3, "tail-snapshot"u8, deliveryToken: 9999UL))
         .IsEqualTo(DatagramSendResult.Queued);
      client.Tick();

      DatagramDeliveryStatus? status = null;
      ulong? token = null;

      // Advance time with ZERO subsequent Data packets sent -> must detect tail loss via lossCutoffTicks!
      for (var t = 0; t < 25 && status is null; t++)
      {
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(30));
         client.Tick();
         server.Tick();
         client.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.DeliveryNotification)
            {
               token = evt.DeliveryToken;
               status = evt.DeliveryStatus;
            }
         });
         server.PollEvents(_ => { });
      }

      await Assert.That(token).IsEqualTo(9999UL);
      await Assert.That(status).IsEqualTo(DatagramDeliveryStatus.Lost);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task UnreliableFragmentedDeliveryNotification_NotifiesLostOnceOnPartialDrop_AndAckedOnceOnComplete()
   {
      var hub = new SimulatedNetworkHub(seed: 6003);
      var (server, client, clientPeer, _) = await ConnectPairAsync(
         hub,
         CreateOptions(allowUnreliableFrag: true),
         CreateOptions(allowUnreliableFrag: true),
         port: 10303);

      var big = new byte[3400]; // 3 fragments
      new Random(7).NextBytes(big);

      // Drop fragment #2 of message A (token 1) -> must notify Lost ONCE
      var pktNum = 0;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (!src.Equals(client.LocalEndPoint)) return true;
         if (!DatagramWireHeader.TryRead(raw, out var hdr) || hdr.Kind != DatagramPacketKind.Data) return true;
         pktNum++;
         return pktNum != 2;
      };

      await Assert.That(clientPeer.TrySend(4, big, deliveryToken: 1UL)).IsEqualTo(DatagramSendResult.Queued);

      var notifications = new List<(ulong Token, DatagramDeliveryStatus Status)>();
      for (var t = 0; t < 30; t++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(30));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(30));
         Drain();
      }

      // Now send message B (token 2) cleanly -> must notify Acknowledged ONCE
      hub.OnPacketSending = null;
      await Assert.That(clientPeer.TrySend(4, big, deliveryToken: 2UL)).IsEqualTo(DatagramSendResult.Queued);
      for (var t = 0; t < 20; t++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(25));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(25));
         Drain();
      }

      await Assert.That(notifications).IsEquivalentTo([
         (1UL, DatagramDeliveryStatus.Lost),
         (2UL, DatagramDeliveryStatus.Acknowledged)
      ]);

      await client.DisposeAsync();
      await server.DisposeAsync();

      void Drain()
      {
         server.PollEvents(_ => { });
         client.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.DeliveryNotification)
               notifications.Add((evt.DeliveryToken, evt.DeliveryStatus));
         });
      }
   }

   [Test]
   public async Task ReliableChannelsDeliveryNotification_TracksRetransmittedFragmentsAndReliableSequencedSupersede()
   {
      var hub = new SimulatedNetworkHub(seed: 6004);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub, port: 10304);

      var bigReliable = new byte[3600]; // ~3 fragments on ReliableOrdered
      new Random(11).NextBytes(bigReliable);

      // Drop first 2 Data packets so Ch0 must retransmit before full acknowledgment
      var dropped = 0;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (src.Equals(client.LocalEndPoint)
             && DatagramWireHeader.TryRead(raw, out var hdr)
             && hdr.Kind == DatagramPacketKind.Data
             && dropped < 2)
         {
            dropped++;
            return false;
         }
         return true;
      };

      await Assert.That(clientPeer.TrySend(0, bigReliable, deliveryToken: 500UL)).IsEqualTo(DatagramSendResult.Queued);

      // ReliableSequenced: enqueue token 600, then immediately supersede with token 601!
      await Assert.That(clientPeer.TrySend(2, "hp=80"u8, deliveryToken: 600UL)).IsEqualTo(DatagramSendResult.Queued);
      await Assert.That(clientPeer.TrySend(2, "hp=60"u8, deliveryToken: 601UL)).IsEqualTo(DatagramSendResult.Queued);

      var notifications = new List<(byte Ch, ulong Token, DatagramDeliveryStatus Status)>();
      for (var t = 0; t < 40 && notifications.Count < 3; t++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(35));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(35));
         server.PollEvents(_ => { });
         client.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.DeliveryNotification)
               notifications.Add((evt.ChannelId, evt.DeliveryToken, evt.DeliveryStatus));
         });
      }

      await Assert.That(dropped).IsEqualTo(2);
      await Assert.That(notifications.Count).IsEqualTo(3);
      await Assert.That(notifications).Contains(((byte)0, 500UL, DatagramDeliveryStatus.Acknowledged));
      await Assert.That(notifications).Contains(((byte)2, 600UL, DatagramDeliveryStatus.Lost));
      await Assert.That(notifications).Contains(((byte)2, 601UL, DatagramDeliveryStatus.Acknowledged));

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task UnreliableDeliveryNotification_RingBufferEviction_NotifiesLostForOverwrittenUnackedTokens()
   {
      var hub = new SimulatedNetworkHub(seed: 6005);
      var (server, client, clientPeer, _) = await ConnectPairAsync(
         hub,
         CreateOptions(maxDatagramsPerTick: 64),
         CreateOptions(maxDatagramsPerTick: 64),
         port: 10305);

      // Blackhole all client -> server packets so nothing is ACKed; send 1040 individual datagrams (>1024 ledger capacity)
      hub.OnPacketSending = (src, _, _) => !src.Equals(client.LocalEndPoint);

      var lostTokens = new HashSet<ulong>();
      for (var i = 1; i <= 1040; i++)
      {
         clientPeer.TrySend(3, BitConverter.GetBytes(i), deliveryToken: (ulong)i);
         client.Tick();
         client.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.DeliveryNotification && evt.DeliveryStatus == DatagramDeliveryStatus.Lost)
               lostTokens.Add(evt.DeliveryToken);
         });
      }

      // First 16 tokens were overwritten in the 1024-slot SentPacketLedger and notified as Lost
      await Assert.That(lostTokens.Count).IsGreaterThanOrEqualTo(16);
      await Assert.That(lostTokens).Contains(1UL);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   [NotInParallel]
   public async Task AsyncDispatchMode_MessageDeliveryUpdatedCallback_FiresOverRealUdpSockets()
   {
      DatagramOptions MakeAsyncOpts() => new DatagramOptions
      {
         DispatchMode = DatagramEventDispatchMode.Async,
         ManualTick = false,
         TickInterval = TimeSpan.FromMilliseconds(5),
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Aead }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered)
      .AddChannel(1, DatagramDeliveryMode.UnreliableSequenced, allowFragmentation: false);

      await using var server = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeAsyncOpts());
      server.Events.MessageReceived = (_, _, _, _, _) => ValueTask.CompletedTask;
      server.Start();

      var ackedTokens = new ConcurrentBag<(byte Ch, ulong Token, DatagramDeliveryStatus Status)>();
      var bothAckedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      await using var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeAsyncOpts());
      client.Events.MessageDeliveryUpdated = (_, ch, token, status, _) =>
      {
         ackedTokens.Add((ch, token, status));
         if (ackedTokens.Count >= 2)
         {
            bothAckedTcs.TrySetResult();
         }
         return ValueTask.CompletedTask;
      };
      client.Start();

      var peer = await client.ConnectAsync(server.LocalEndPoint!);
      peer.TrySend(0, "rel-msg"u8, deliveryToken: 42UL);
      peer.TrySend(1, "unrel-snap"u8, deliveryToken: 43UL);

      await bothAckedTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
      await Assert.That(ackedTokens).Contains(((byte)0, 42UL, DatagramDeliveryStatus.Acknowledged));
      await Assert.That(ackedTokens).Contains(((byte)1, 43UL, DatagramDeliveryStatus.Acknowledged));
   }

   [Test]
   public async Task MaxDatagramsPerTick_ValidatesBoundsAndControlsPerTickFlushCeiling()
   {
      await Assert.That(() => new DatagramHost(new DatagramOptions { MaxDatagramsPerTick = 0 }))
         .Throws<ArgumentOutOfRangeException>();

      var payload900 = new byte[900]; // 1 message per 1200B datagram

      // 1. Cap = 4 -> single Tick() emits at most 4 Data datagrams
      {
         var hub = new SimulatedNetworkHub(seed: 6010);
         var (server, client, peer, _) = await ConnectPairAsync(
            hub,
            CreateOptions(maxDatagramsPerTick: 4),
            CreateOptions(maxDatagramsPerTick: 4),
            port: 10310);

         for (var i = 0; i < 20; i++) peer.TrySend(4, payload900);
         var before = peer.Stats.DatagramsSent;
         client.Tick();
         var sentInOneTick = peer.Stats.DatagramsSent - before;

         await Assert.That(sentInOneTick).IsEqualTo(4L);
         await client.DisposeAsync();
         await server.DisposeAsync();
      }

      // 2. Cap = 64 -> single Tick() flushes all 50 Data datagrams at once (above old hardcoded 32 limit!)
      {
         var hub = new SimulatedNetworkHub(seed: 6011);
         var (server, client, peer, _) = await ConnectPairAsync(
            hub,
            CreateOptions(maxDatagramsPerTick: 64),
            CreateOptions(maxDatagramsPerTick: 64),
            port: 10311);

         for (var i = 0; i < 50; i++) peer.TrySend(4, payload900);
         var before = peer.Stats.DatagramsSent;
         client.Tick();
         var sentInOneTick = peer.Stats.DatagramsSent - before;

         await Assert.That(sentInOneTick).IsEqualTo(50L);
         await client.DisposeAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task RendezvousServer_EnforcesMaxRegistrationsAndTtlExpiry()
   {
      var hub = new SimulatedNetworkHub(seed: 6020);
      var rdvEp = new IPEndPoint(IPAddress.Loopback, 10320);
      var rdvOpts = new DatagramOptions { Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Crc32C } };

      await using var rdv = new DatagramRendezvousServer(rdvEp, hub.CreateTransport(), rdvOpts, hub.Time);
      await Assert.That(() => rdv.MaxRegistrations = 0).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => rdv.RegistrationTtl = TimeSpan.Zero).Throws<ArgumentOutOfRangeException>();

      rdv.MaxRegistrations = 3;
      rdv.RegistrationTtl = TimeSpan.FromSeconds(5);
      rdv.Start();

      // Register 5 distinct nodes sequentially -> RegistrationCount must never exceed MaxRegistrations (3)!
      var clients = new DatagramHost[5];
      for (var i = 0; i < 5; i++)
      {
         clients[i] = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateOptions(), hub.CreateTransport(), hub.Time);
         clients[i].Start();

         var discTask = clients[i].DiscoverPublicEndPointAsync(rdvEp, System.Text.Encoding.UTF8.GetBytes($"node-{i}")).AsTask();
         for (var t = 0; t < 20 && !discTask.IsCompleted; t++)
         {
            clients[i].Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            await Task.Delay(5);
         }

         var mapped = await discTask.WaitAsync(TimeSpan.FromSeconds(2));
         await Assert.That(mapped).IsEqualTo(clients[i].LocalEndPoint);
         await Assert.That(rdv.RegistrationCount).IsLessThanOrEqualTo(3);
      }

      await Assert.That(rdv.RegistrationCount).IsEqualTo(3);

      // Advance past RegistrationTtl (5s -> advance 6s) and verify introduction refuses expired peers + Purge clears remaining!
      hub.AdvanceAndDeliver(TimeSpan.FromSeconds(6));

      var introFired = false;
      clients[3].Events.PunchIntroduced = (_, _) => { introFired = true; return ValueTask.CompletedTask; };
      clients[3].RequestIntroduction(rdvEp, "node-3"u8, "node-4"u8, 0x1234UL);

      for (var t = 0; t < 10; t++)
      {
         clients[3].Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         await Task.Delay(5);
      }

      await Assert.That(introFired).IsFalse();
      // Both node-3 and node-4 were evicted during introduction check, and remaining node-2 purges cleanly
      var remainingPurged = rdv.PurgeExpiredRegistrations();
      await Assert.That(remainingPurged).IsGreaterThanOrEqualTo(1);
      await Assert.That(rdv.RegistrationCount).IsEqualTo(0);

      foreach (var c in clients) await c.DisposeAsync();
   }

   [Test]
   public async Task DiscoverPublicEndPointAsync_RetriesAutomaticallyUnderPacketLoss_AndReturnsNullOnTimeout()
   {
      var hub = new SimulatedNetworkHub(seed: 6030);
      var rdvEp = new IPEndPoint(IPAddress.Loopback, 10330);
      var rdvOpts = new DatagramOptions { Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Crc32C } };

      await using var rdv = new DatagramRendezvousServer(rdvEp, hub.CreateTransport(), rdvOpts, hub.Time);
      rdv.Start();

      var clientOpts = CreateOptions();
      clientOpts.HandshakeRetryInterval = TimeSpan.FromMilliseconds(100);
      await using var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), clientOpts, hub.CreateTransport(), hub.Time);
      client.Start();

      // 1. Drop first 2 RendezvousRegister requests AND first RendezvousMapped response -> retries MUST succeed!
      var regDropped = 0;
      var mapDropped = 0;
      hub.OnPacketSending = (_, _, raw) =>
      {
         if (!DatagramWireHeader.TryRead(raw, out var hdr)) return true;
         if (hdr.Kind == DatagramPacketKind.RendezvousRegister && regDropped < 2)
         {
            regDropped++;
            return false;
         }
         if (hdr.Kind == DatagramPacketKind.RendezvousMapped && mapDropped < 1)
         {
            mapDropped++;
            return false;
         }
         return true;
      };

      var discTask = client.DiscoverPublicEndPointAsync(rdvEp, "resilient-node"u8.ToArray(), timeout: TimeSpan.FromSeconds(2)).AsTask();
      for (var t = 0; t < 40 && !discTask.IsCompleted; t++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(50));
         await Task.Delay(5);
      }

      var mapped = await discTask.WaitAsync(TimeSpan.FromSeconds(2));
      await Assert.That(regDropped).IsEqualTo(2);
      await Assert.That(mapDropped).IsEqualTo(1);
      await Assert.That(mapped).IsEqualTo(client.LocalEndPoint);

      // 2. Drop ALL packets -> DiscoverPublicEndPointAsync times out cleanly and returns null
      hub.OnPacketSending = (_, _, _) => false;
      var timeoutTask = client.DiscoverPublicEndPointAsync(rdvEp, "unreachable-node"u8.ToArray(), timeout: TimeSpan.FromMilliseconds(350)).AsTask();
      for (var t = 0; t < 20 && !timeoutTask.IsCompleted; t++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(40));
      }

      var timedOutResult = await timeoutTask.WaitAsync(TimeSpan.FromSeconds(2));
      await Assert.That(timedOutResult).IsNull();
   }
}
