using System.Buffers.Binary;
using System.Net;
using System.Text;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Protocol;
using Beskar.Networking.Datagram.Reliability;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Networking.Datagram.Tests;

public class CongestionPacingEdgeCaseAndOptionsTests
{
   [Test]
   public async Task PriorityPacking_PacksHigherPriorityChannelsAheadOfLowerPriorityUnderMtuCap()
   {
      var hub = new SimulatedNetworkHub(seed: 701);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9800);

      DatagramOptions MakeOpts() => new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxDatagramSize = 600,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero,
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Crc32C }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered, priority: 10)   // Low priority
      .AddChannel(1, DatagramDeliveryMode.ReliableOrdered, priority: 250); // High priority

      var server = new DatagramHost(serverEp, MakeOpts(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts(), hub.CreateTransport(), hub.Time);
      server.Start(); client.Start();

      var connectTask = client.ConnectAsync(serverEp);
      for (var i = 0; i < 20 && !connectTask.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var peer = await connectTask;
      server.PollEvents(_ => { });

      // Enqueue a 450B low-priority message FIRST, then a 450B high-priority message SECOND (can't both fit in 600B!)
      var lowPayload = new byte[450];
      var highPayload = new byte[450];
      Array.Fill(lowPayload, (byte)0xAA);
      Array.Fill(highPayload, (byte)0xBB);

      peer.TrySend(0, lowPayload);
      peer.TrySend(1, highPayload);

      // Capture only the FIRST datagram emitted by client.Tick()
      var dataPacketsSent = 0;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (!src.Equals(client.LocalEndPoint)) return true;
         if (!DatagramWireHeader.TryRead(raw, out var hdr) || hdr.Kind != DatagramPacketKind.Data) return true;
         dataPacketsSent++;
         return dataPacketsSent == 1; // allow ONLY the 1st datagram through!
      };

      client.Tick();
      server.Tick();

      var deliveredChannels = new List<byte>();
      server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived) deliveredChannels.Add(evt.ChannelId);
      });

      // The 1st datagram MUST have packed Channel 1 (priority 250), NOT Channel 0 (priority 10)!
      await Assert.That(deliveredChannels).IsEquivalentTo([(byte)1]);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task TokenBucketPacer_ThrottlesBurstAndSmoothsDeliveryAcrossTime()
   {
      var hub = new SimulatedNetworkHub(seed: 702);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9801);

      DatagramOptions MakeOpts() => new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxDatagramSize = 1000,
         MaxBytesPerSecondPerPeer = 10_000, // 10 KB/s
         PacingBurstBytes = 2500,           // Only ~2-3 datagrams per burst!
         TimeSyncInterval = TimeSpan.Zero,
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Crc32C }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered);

      var server = new DatagramHost(serverEp, MakeOpts(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts(), hub.CreateTransport(), hub.Time);
      server.Start(); client.Start();

      var t = client.ConnectAsync(serverEp);
      for (var i = 0; i < 20 && !t.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var peer = await t;
      server.PollEvents(_ => { });

      // Queue 10 x 900B messages (~9 KB total)
      var chunk = new byte[900];
      for (var i = 0; i < 10; i++)
      {
         peer.TrySend(0, chunk);
      }

      // Single tick at t=0 must NOT blast all 10 datagrams (capped by 2500B burst)
      client.Tick();
      var afterFirstTick = peer.Stats.DatagramsSent;
      await Assert.That(afterFirstTick).IsLessThanOrEqualTo(4);

      // Advancing 1.2 seconds refills 12 KB of tokens -> all 10 messages arrive smoothly!
      var received = 0;
      for (var i = 0; i < 60 && received < 10; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(25));
         server.Tick();
         server.PollEvents(evt => { if (evt.Type == DatagramEventType.MessageReceived) received++; });
         client.PollEvents(_ => { });
      }

      await Assert.That(received).IsEqualTo(10);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task MaxReliableRetryDuration_DisconnectsPeerEvenWhileKeepAlivesPreventPeerTimeout()
   {
      var hub = new SimulatedNetworkHub(seed: 703);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9802);

      DatagramOptions MakeOpts() => new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         PeerTimeout = TimeSpan.FromSeconds(30),               // Long peer timeout
         MaxReliableRetryDuration = TimeSpan.FromMilliseconds(600), // Short reliable retry limit
         KeepAliveInterval = TimeSpan.FromMilliseconds(100),
         MinRtoMs = 50,
         MaxRtoMs = 150,
         TimeSyncInterval = TimeSpan.Zero,
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Crc32C }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered);

      var server = new DatagramHost(serverEp, MakeOpts(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts(), hub.CreateTransport(), hub.Time);
      server.Start(); client.Start();

      var t = client.ConnectAsync(serverEp);
      for (var i = 0; i < 20 && !t.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var peer = await t;
      client.PollEvents(_ => { });
      server.PollEvents(_ => { });

      // Drop ONLY Data packets from client, while letting AckOnly keepalives flow freely!
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (src.Equals(client.LocalEndPoint) && DatagramWireHeader.TryRead(raw, out var hdr) && hdr.Kind == DatagramPacketKind.Data)
         {
            return false;
         }
         return true;
      };

      peer.TrySend(0, "will-never-ack"u8);

      DatagramDisconnectReason? reason = null;
      for (var i = 0; i < 40 && reason is null; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(30));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(30));
         client.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.PeerDisconnected) reason = evt.DisconnectReason;
         });
         server.PollEvents(_ => { });
      }

      await Assert.That(reason).IsEqualTo(DatagramDisconnectReason.MaxRetriesExceeded);
      await Assert.That(client.PeerCount).IsEqualTo(0);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task CongestionStateAndTimeSyncClockOffset_TrackLinkHealthAndClockDelta()
   {
      var timeA = new SimulatedTimeProvider(new DateTimeOffset(2026, 6, 1, 12, 0, 0, TimeSpan.Zero));
      var timeB = new SimulatedTimeProvider(new DateTimeOffset(2026, 6, 1, 12, 0, 0, 500, TimeSpan.Zero)); // +500ms ahead!

      var hub = new SimulatedNetworkHub(seed: 704, chaos: new SimulatedChaosProfile
      {
         MinLatency = TimeSpan.FromMilliseconds(20),
         MaxLatency = TimeSpan.FromMilliseconds(20)
      }, time: timeA);

      var serverEp = new IPEndPoint(IPAddress.Loopback, 9803);
      DatagramOptions MakeOpts() => new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.FromMilliseconds(100),
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Crc32C }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered)
      .AddChannel(1, DatagramDeliveryMode.UnreliableSequenced);

      var server = new DatagramHost(serverEp, MakeOpts(), hub.CreateTransport(), timeB);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts(), hub.CreateTransport(), timeA);
      server.Start(); client.Start();

      var t = client.ConnectAsync(serverEp);
      for (var i = 0; i < 30 && !t.IsCompleted; i++)
      {
         client.Tick();
         timeA.Advance(TimeSpan.FromMilliseconds(20));
         timeB.Advance(TimeSpan.FromMilliseconds(20));
         hub.DeliverDue();
         server.Tick();
      }
      var peer = await t;
      client.PollEvents(_ => { });
      server.PollEvents(_ => { });

      // Pump clean traffic + TimeSync probes -> Healthy state, RTT ~40ms, ClockOffset ~ +500ms!
      for (var i = 0; i < 40; i++)
      {
         peer.TrySend(1, BitConverter.GetBytes(i));
         client.Tick();
         timeA.Advance(TimeSpan.FromMilliseconds(30));
         timeB.Advance(TimeSpan.FromMilliseconds(30));
         hub.DeliverDue();
         server.Tick();
         server.PollEvents(_ => { });
         client.PollEvents(_ => { });
      }

      var cleanStats = peer.Stats;
      await Assert.That(cleanStats.CongestionState).IsEqualTo(DatagramCongestionState.Healthy);
      await Assert.That(cleanStats.RecommendedSendScale).IsEqualTo(1.0f);
      await Assert.That(cleanStats.SmoothedRttMs).IsGreaterThanOrEqualTo(20.0);
      await Assert.That(Math.Abs(TimeSpan.FromTicks(cleanStats.EstimatedClockOffsetTicks).TotalMilliseconds - 500.0)).IsLessThan(60.0);

      // Now inject 35% sustained loss -> CongestionState must degrade to Congested or Critical!
      hub.Chaos.PacketDropRate = 0.35;
      for (var i = 0; i < 120; i++)
      {
         peer.TrySend(0, BitConverter.GetBytes(i));
         client.Tick();
         timeA.Advance(TimeSpan.FromMilliseconds(25));
         timeB.Advance(TimeSpan.FromMilliseconds(25));
         hub.DeliverDue();
         server.Tick();
         server.PollEvents(_ => { });
         client.PollEvents(_ => { });
      }

      var lossStats = peer.Stats;
      await Assert.That(lossStats.PacketLossRate).IsGreaterThan(0.05);
      await Assert.That(lossStats.CongestionState).IsNotEqualTo(DatagramCongestionState.Healthy);
      await Assert.That(lossStats.RecommendedSendScale).IsLessThan(1.0f);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task SendValidation_PeerProperties_SendToSubset_AndJumboDatagram_WorkCleanly()
   {
      // Validate DatagramOptions guards
      var badMtu = new DatagramOptions { MaxDatagramSize = 100 };
      await Assert.That(() => new DatagramHost(badMtu)).Throws<ArgumentOutOfRangeException>();

      var badPacing = new DatagramOptions { MaxBytesPerSecondPerPeer = 0 };
      await Assert.That(() => new DatagramHost(badPacing)).Throws<ArgumentOutOfRangeException>();

      var dupCh = new DatagramOptions()
         .AddChannel(0, DatagramDeliveryMode.ReliableOrdered)
         .AddChannel(0, DatagramDeliveryMode.UnreliableSequenced);
      await Assert.That(() => new DatagramHost(dupCh)).Throws<InvalidOperationException>();

      var badChId = new DatagramOptions().AddChannel(64, DatagramDeliveryMode.ReliableOrdered);
      await Assert.That(() => new DatagramHost(badChId)).Throws<ArgumentOutOfRangeException>();

      var badWin = new DatagramOptions().AddChannel(0, DatagramDeliveryMode.ReliableOrdered, windowSize: 0);
      await Assert.That(() => new DatagramHost(badWin)).Throws<ArgumentOutOfRangeException>();

      // Jumbo datagram (8000B > 2048 stack threshold) + SendTo subset + Properties + SendResult error codes
      var hub = new SimulatedNetworkHub(seed: 705);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9804);

      DatagramOptions MakeJumboOpts() => new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxDatagramSize = 8000,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered, maxFragmentCount: 2)
      .AddChannel(1, DatagramDeliveryMode.UnreliableSequenced, allowFragmentation: false);

      var server = new DatagramHost(serverEp, MakeJumboOpts(), hub.CreateTransport(), hub.Time);
      var c1 = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeJumboOpts(), hub.CreateTransport(), hub.Time);
      var c2 = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeJumboOpts(), hub.CreateTransport(), hub.Time);
      var c3 = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeJumboOpts(), hub.CreateTransport(), hub.Time);
      server.Start(); c1.Start(); c2.Start(); c3.Start();

      var t1 = c1.ConnectAsync(serverEp);
      var t2 = c2.ConnectAsync(serverEp);
      var t3 = c3.ConnectAsync(serverEp);
      for (var i = 0; i < 25 && (!t1.IsCompleted || !t2.IsCompleted || !t3.IsCompleted); i++)
      {
         c1.Tick(); c2.Tick(); c3.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }

      var p1 = await t1;
      await t2;
      await t3;

      // Test Peer Properties
      p1.SetProperty("score", 42);
      await Assert.That(p1.TryGetProperty<int>("score", out var score)).IsTrue();
      await Assert.That(score).IsEqualTo(42);
      await Assert.That(p1.TryGetProperty<string>("score", out _)).IsFalse();
      await Assert.That(p1.TryGetProperty<int>("missing", out _)).IsFalse();

      // Test SendResult error codes
      await Assert.That(p1.TrySend(99, "x"u8)).IsEqualTo(DatagramSendResult.InvalidChannel);
      await Assert.That(p1.TrySend(5, "x"u8)).IsEqualTo(DatagramSendResult.InvalidChannel);
      await Assert.That(p1.TrySend(1, new byte[8100])).IsEqualTo(DatagramSendResult.PayloadTooLarge); // frag disabled
      await Assert.That(p1.TrySend(0, new byte[25_000])).IsEqualTo(DatagramSendResult.PayloadTooLarge); // exceeds maxFragmentCount=2

      // Jumbo single-packet send (6000B fits in 1 datagram with MaxDatagramSize=8000!)
      var jumbo = new byte[6000];
      new Random(7).NextBytes(jumbo);
      await Assert.That(p1.TrySend(0, jumbo)).IsEqualTo(DatagramSendResult.Queued);

      // Server SendTo targeting ONLY peers [1, 3] (excluding peer 2!)
      server.SendTo([1u, 3u], 0, "targeted-subset"u8);

      byte[]? serverGotJumbo = null;
      string? c1Got = null;
      string? c2Got = null;
      string? c3Got = null;

      for (var i = 0; i < 15; i++)
      {
         c1.Tick(); c2.Tick(); c3.Tick();
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.PollEvents(evt => { if (evt.Type == DatagramEventType.MessageReceived) serverGotJumbo = evt.Payload.ToArray(); });
         c1.PollEvents(evt => { if (evt.Type == DatagramEventType.MessageReceived) c1Got = Encoding.UTF8.GetString(evt.Payload.Span); });
         c2.PollEvents(evt => { if (evt.Type == DatagramEventType.MessageReceived) c2Got = Encoding.UTF8.GetString(evt.Payload.Span); });
         c3.PollEvents(evt => { if (evt.Type == DatagramEventType.MessageReceived) c3Got = Encoding.UTF8.GetString(evt.Payload.Span); });
      }

      await Assert.That(serverGotJumbo).IsEquivalentTo(jumbo);
      await Assert.That(c1Got).IsEqualTo("targeted-subset");
      await Assert.That(c2Got).IsNull();
      await Assert.That(c3Got).IsEqualTo("targeted-subset");

      // Disconnected peer rejects TrySend with PeerNotConnected
      p1.Disconnect();
      c1.Tick();
      await Assert.That(p1.TrySend(0, "after-close"u8)).IsEqualTo(DatagramSendResult.PeerNotConnected);

      await c1.DisposeAsync();
      await c2.DisposeAsync();
      await c3.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task FragmentReassembler_HandlesEvictionConflictsEmptyChunksAndInvalidIndices()
   {
      using var reassembler = new FragmentReassembler(maxInFlightGroups: 2, maxFragmentCount: 4);

      // Invalid fragCount / fragIndex rejected
      await Assert.That(reassembler.TryAddFragment(1, 0, 0, "a"u8, out _)).IsFalse();
      await Assert.That(reassembler.TryAddFragment(1, 0, 5, "a"u8, out _)).IsFalse();
      await Assert.That(reassembler.TryAddFragment(1, 2, 2, "a"u8, out _)).IsFalse();

      // Add group 1 (frag 0/2) and group 2 (frag 0/2), then group 3 -> evicts oldest group (group 1)!
      await Assert.That(reassembler.TryAddFragment(1, 0, 2, "G1-0"u8, out _)).IsFalse();
      await Assert.That(reassembler.TryAddFragment(2, 0, 2, "G2-0"u8, out _)).IsFalse();
      await Assert.That(reassembler.TryAddFragment(3, 0, 2, "G3-0"u8, out _)).IsFalse();

      // Group 1 was evicted, so sending frag 1/2 starts a new incomplete group 1 rather than assembling
      await Assert.That(reassembler.TryAddFragment(1, 1, 2, "G1-1"u8, out _)).IsFalse();

      // Conflicting FragCount for group 3 resets it cleanly
      await Assert.That(reassembler.TryAddFragment(3, 1, 3, "bad"u8, out _)).IsFalse();

      // Empty chunk + non-empty chunk reassemble cleanly
      await Assert.That(reassembler.TryAddFragment(10, 0, 2, ReadOnlySpan<byte>.Empty, out _)).IsFalse();
      await Assert.That(reassembler.TryAddFragment(10, 1, 2, "ok"u8, out var lease)).IsTrue();
      using (lease)
      {
         await Assert.That(Encoding.UTF8.GetString(lease.Span)).IsEqualTo("ok");
      }
   }

   [Test]
   public async Task RendezvousCodec_RoundTripsIpv4AndIpv6EndPoints_AndPollQueueOverflowDropsSafely()
   {
      var buf = new byte[256];
      var nodeId = "node-ipv6"u8.ToArray();
      var v6Ep = new IPEndPoint(IPAddress.Parse("2001:db8::42"), 12345);
      var v4Ep = new IPEndPoint(IPAddress.Parse("192.168.1.50"), 7777);

      var regLen = Beskar.Networking.Datagram.Nat.DatagramRendezvousCodec.WriteRegister(buf, nodeId, v6Ep);
      var regOk = Beskar.Networking.Datagram.Nat.DatagramRendezvousCodec.TryReadRegister(buf.AsSpan(0, regLen), out var decNode, out var decLan);
      var decNodeArr = decNode.ToArray();
      await Assert.That(regOk).IsTrue();
      await Assert.That(decNodeArr).IsEquivalentTo(nodeId);
      await Assert.That(decLan).IsEqualTo(v6Ep);

      var mapLen = Beskar.Networking.Datagram.Nat.DatagramRendezvousCodec.WriteMapped(buf, nodeId, v6Ep);
      var mapOk = Beskar.Networking.Datagram.Nat.DatagramRendezvousCodec.TryReadMapped(buf.AsSpan(0, mapLen), out _, out var decPub);
      await Assert.That(mapOk).IsTrue();
      await Assert.That(decPub).IsEqualTo(v6Ep);

      var introLen = Beskar.Networking.Datagram.Nat.DatagramRendezvousCodec.WriteIntroduceNotify(buf, nodeId, 0xABCDUL, v6Ep, v4Ep);
      var introOk = Beskar.Networking.Datagram.Nat.DatagramRendezvousCodec.TryReadIntroduceNotify(buf.AsSpan(0, introLen), out _, out var decTok, out var decIntroPub, out var decIntroLan);
      await Assert.That(introOk).IsTrue();
      await Assert.That(decTok).IsEqualTo(0xABCDUL);
      await Assert.That(decIntroPub).IsEqualTo(v6Ep);
      await Assert.That(decIntroLan).IsEqualTo(v4Ep);

      // Verify MaxQueuedEvents cap drops excess poll events cleanly without throwing
      var hub = new SimulatedNetworkHub(seed: 709);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9809);
      DatagramOptions MakeSmallQueueOpts() => new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxQueuedEvents = 5,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero,
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Crc32C }
      }.AddChannel(0, DatagramDeliveryMode.UnreliableUnordered);

      var server = new DatagramHost(serverEp, MakeSmallQueueOpts(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeSmallQueueOpts(), hub.CreateTransport(), hub.Time);
      server.Start(); client.Start();

      var t = client.ConnectAsync(serverEp);
      for (var i = 0; i < 15 && !t.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var peer = await t;
      server.PollEvents(_ => { });

      for (var i = 0; i < 20; i++) peer.TrySend(0, BitConverter.GetBytes(i));
      client.Tick();
      server.Tick();

      var drained = server.PollEvents(_ => { });
      await Assert.That(drained).IsEqualTo(5);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}
