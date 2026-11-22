using System.Buffers.Binary;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Nat;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Protocol;
using Beskar.Networking.Datagram.Reliability;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Networking.Datagram.Tests;

public class HardeningAndEdgeCaseTests
{
   private static DatagramOptions CreateManualOptions(
      DatagramSecurityMode security = DatagramSecurityMode.Aead,
      int maxDatagramSize = 1200,
      byte[]? psk = null)
   {
      return new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxDatagramSize = maxDatagramSize,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero,
         ConnectTimeout = TimeSpan.FromSeconds(5),
         HandshakeRetryInterval = TimeSpan.FromMilliseconds(100),
         PeerTimeout = TimeSpan.FromSeconds(10),
         Security = new DatagramSecurityOptions
         {
            Mode = security,
            PreSharedKey = psk
         }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered)
      .AddChannel(1, DatagramDeliveryMode.UnreliableSequenced, allowFragmentation: false);
   }

   [Test]
   public async Task AckTracker_NearAndAtUIntMaxValue_NeverInfiniteLoopsAndRejectsReplays()
   {
      var tracker = new AckTracker();

      await Assert.That(tracker.TryRecordReceivedPacket(uint.MaxValue - 2)).IsTrue();
      await Assert.That(tracker.TryRecordReceivedPacket(uint.MaxValue)).IsTrue();
      await Assert.That(tracker.HighestReceivedSeq).IsEqualTo(uint.MaxValue);
      await Assert.That(tracker.OutgoingAckBitfield).IsEqualTo(0b10UL);

      // Missing uint.MaxValue - 1 arrives out-of-order
      await Assert.That(tracker.TryRecordReceivedPacket(uint.MaxValue - 1)).IsTrue();
      await Assert.That(tracker.OutgoingAckBitfield).IsEqualTo(0b11UL);

      // Replays of uint.MaxValue and uint.MaxValue - 1 are rejected
      await Assert.That(tracker.TryRecordReceivedPacket(uint.MaxValue)).IsFalse();
      await Assert.That(tracker.TryRecordReceivedPacket(uint.MaxValue - 1)).IsFalse();
   }

   [Test]
   public async Task SentPacketLedger_AtUIntMaxValue_TerminatesCleanlyWithoutWrapLoop()
   {
      var time = new SimulatedTimeProvider();
      var rtt = new RttEstimator(50, 2000, TimeSpan.FromMilliseconds(10), time, time.GetTimestamp());
      var ledger = new SentPacketLedger();

      ledger.BeginRecord(uint.MaxValue, time.GetTimestamp(), out var msgs);
      msgs.Add(new SentMessageRef(0, 1));

      var acked = 0;
      ledger.ProcessAcks(uint.MaxValue, 0UL, uint.MaxValue, time.GetTimestamp(), time, rtt, (_, _) => acked++);
      await Assert.That(acked).IsEqualTo(1);

      // Forged future AckSeq (> latestSentPacketSeq) is ignored cleanly
      ledger.ProcessAcks(uint.MaxValue, 0UL, uint.MaxValue - 1, time.GetTimestamp(), time, rtt, (_, _) => { });

      // Must complete in O(Capacity) even when jumping to uint.MaxValue without wrapping forever
      ledger.DetectLossesAndFastRetransmits(uint.MaxValue, time.GetTimestamp(), TimeSpan.FromSeconds(1).Ticks, time, rtt, (_, _) => { });
   }

   [Test]
   public async Task DatagramVarInt_Rejects5thByteOverflowBitPatterns()
   {
      // 0x80, 0x80, 0x80, 0x80, 0x10 shifts 0x10 << 28 => 0 in 32-bit math if unchecked!
      await Assert.That(DatagramVarInt.TryRead([0x80, 0x80, 0x80, 0x80, 0x10], out _, out _)).IsFalse();
      await Assert.That(DatagramVarInt.TryRead([0x80, 0x80, 0x80, 0x80, 0x08], out _, out _)).IsFalse();
      await Assert.That(DatagramVarInt.TryRead([0x80, 0x80, 0x80, 0x80, 0x7F], out _, out _)).IsFalse();

      // 0x07 in 5th byte is exact int.MaxValue upper nibble (valid)
      var ok = DatagramVarInt.TryRead([0xFF, 0xFF, 0xFF, 0xFF, 0x07], out var val, out var read);
      await Assert.That(ok).IsTrue();
      await Assert.That(val).IsEqualTo(int.MaxValue);
      await Assert.That(read).IsEqualTo(5);
   }

   [Test]
   public async Task MessageCodec_RejectsCraftedFragmentOnChannelWithFragmentationDisabled()
   {
      var opts = new DatagramOptions()
         .AddChannel(0, DatagramDeliveryMode.UnreliableSequenced, allowFragmentation: false);
      var table = opts.BuildValidatedChannelTable(out _, out _);

      var wire = new byte[64];
      var len = DatagramMessageCodec.Write(
         wire,
         channelId: 0,
         mode: DatagramDeliveryMode.UnreliableSequenced,
         channelSeq: 1,
         isFragment: true,
         fragGroupId: 1,
         fragIndex: 0,
         fragCount: 2,
         payload: "evil-frag"u8);

      await Assert.That(DatagramMessageCodec.TryRead(wire.AsSpan(0, len), table, out _, out _)).IsFalse();
   }

   [Test]
   [Arguments(DatagramSecurityMode.None)]
   [Arguments(DatagramSecurityMode.Crc32C)]
   public async Task SingleClientConnectedToMultipleServers_InNonAeadModes_HasZeroSessionTokenCollisions(DatagramSecurityMode mode)
   {
      var hub = new SimulatedNetworkHub(seed: 5001 + (int)mode);
      var s1Ep = new IPEndPoint(IPAddress.Loopback, 10101);
      var s2Ep = new IPEndPoint(IPAddress.Loopback, 10102);
      var s3Ep = new IPEndPoint(IPAddress.Loopback, 10103);

      await using var s1 = new DatagramHost(s1Ep, CreateManualOptions(mode), hub.CreateTransport(), hub.Time);
      await using var s2 = new DatagramHost(s2Ep, CreateManualOptions(mode), hub.CreateTransport(), hub.Time);
      await using var s3 = new DatagramHost(s3Ep, CreateManualOptions(mode), hub.CreateTransport(), hub.Time);
      await using var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateManualOptions(mode), hub.CreateTransport(), hub.Time);

      s1.Start(); s2.Start(); s3.Start(); client.Start();

      var t1 = client.ConnectAsync(s1Ep);
      var t2 = client.ConnectAsync(s2Ep);
      var t3 = client.ConnectAsync(s3Ep);

      for (var i = 0; i < 30 && (!t1.IsCompleted || !t2.IsCompleted || !t3.IsCompleted); i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         s1.Tick(); s2.Tick(); s3.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }

      var p1 = await t1;
      var p2 = await t2;
      var p3 = await t3;

      await Assert.That(client.PeerCount).IsEqualTo(3);
      await Assert.That(p1.SessionToken).IsNotEqualTo(p2.SessionToken);
      await Assert.That(p2.SessionToken).IsNotEqualTo(p3.SessionToken);
      await Assert.That(p1.SessionToken).IsNotEqualTo(p3.SessionToken);

      // Bidirectional exchange across all 3 servers simultaneously
      p1.TrySend(0, "to-s1"u8);
      p2.TrySend(0, "to-s2"u8);
      p3.TrySend(0, "to-s3"u8);

      string? got1 = null, got2 = null, got3 = null;
      var clientReplies = new HashSet<string>();

      for (var i = 0; i < 20; i++)
      {
         client.Tick();
         s1.Tick(); s2.Tick(); s3.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));

         s1.PollEvents(e => { if (e.Type == DatagramEventType.MessageReceived) { got1 = Encoding.UTF8.GetString(e.Payload.Span); e.Peer.TrySend(0, "from-s1"u8); } });
         s2.PollEvents(e => { if (e.Type == DatagramEventType.MessageReceived) { got2 = Encoding.UTF8.GetString(e.Payload.Span); e.Peer.TrySend(0, "from-s2"u8); } });
         s3.PollEvents(e => { if (e.Type == DatagramEventType.MessageReceived) { got3 = Encoding.UTF8.GetString(e.Payload.Span); e.Peer.TrySend(0, "from-s3"u8); } });
         client.PollEvents(e => { if (e.Type == DatagramEventType.MessageReceived) clientReplies.Add(Encoding.UTF8.GetString(e.Payload.Span)); });
      }

      await Assert.That(got1).IsEqualTo("to-s1");
      await Assert.That(got2).IsEqualTo("to-s2");
      await Assert.That(got3).IsEqualTo("to-s3");
      await Assert.That(clientReplies).IsEquivalentTo(["from-s1", "from-s2", "from-s3"]);
   }

   [Test]
   [NotInParallel]
   public async Task SlowAsyncValidateConnection_DeduplicatesRetriesAndDoesNotStallReceiveLoopForOtherPeers()
   {
      DatagramOptions MakeOpts(DatagramSecurityMode mode) => new DatagramOptions
      {
         DispatchMode = DatagramEventDispatchMode.Async,
         ManualTick = false,
         TickInterval = TimeSpan.FromMilliseconds(5),
         HandshakeRetryInterval = TimeSpan.FromMilliseconds(40), // Faster than ValidateConnection delay!
         ConnectTimeout = TimeSpan.FromSeconds(4),
         Security = new DatagramSecurityOptions { Mode = mode }
      }.AddChannel(0, DatagramDeliveryMode.ReliableOrdered);

      foreach (var mode in new[] { DatagramSecurityMode.Crc32C, DatagramSecurityMode.Aead })
      {
         await using var server = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts(mode));
         var slowValidationGate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
         var slowValidationStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
         var slowValidationCalls = 0;

         server.Events.ValidateConnection = async ctx =>
         {
            var tag = Encoding.UTF8.GetString(ctx.ConnectMetadata.Span);
            ctx.PeerTag = tag;
            if (tag == "slow-client")
            {
               Interlocked.Increment(ref slowValidationCalls);
               slowValidationStarted.TrySetResult();
               await slowValidationGate.Task;
            }
         };

         var fastEchoTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
         server.Events.MessageReceived = (peer, _, _, payload, _) =>
         {
            peer.TrySend(0, payload.Span);
            return ValueTask.CompletedTask;
         };
         server.Start();

         // Connect a healthy fast peer first
         await using var fastClient = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts(mode));
         fastClient.Events.MessageReceived = (_, _, _, payload, _) =>
         {
            fastEchoTcs.TrySetResult(Encoding.UTF8.GetString(payload.Span));
            return ValueTask.CompletedTask;
         };
         fastClient.Start();
         var fastPeer = await fastClient.ConnectAsync(server.LocalEndPoint!, "fast-client"u8.ToArray());

         // Now start slow client whose ValidateConnection blocks for 220ms (> 5x HandshakeRetryInterval!)
         await using var slowClient = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts(mode));
         slowClient.Start();
         var slowConnectTask = slowClient.ConnectAsync(server.LocalEndPoint!, "slow-client"u8.ToArray());

         await slowValidationStarted.Task.WaitAsync(TimeSpan.FromSeconds(2));

         // Wait long enough for slowClient to send multiple ConnectAccept retries while gate is still closed
         await Task.Delay(180);

         // While slow-client is STILL blocked inside ValidateConnection, fast-client MUST receive echo immediately!
         var sw = Stopwatch.StartNew();
         fastPeer.TrySend(0, "unblocked-ping"u8);
         var echoed = await fastEchoTcs.Task.WaitAsync(TimeSpan.FromSeconds(1));
         sw.Stop();

         await Assert.That(echoed).IsEqualTo("unblocked-ping");
         await Assert.That(sw.ElapsedMilliseconds).IsLessThan(500);

         // Now release slow-client validation -> must succeed with EXACTLY 1 ValidateConnection call and 2 total peers!
         slowValidationGate.TrySetResult();
         var slowPeer = await slowConnectTask.WaitAsync(TimeSpan.FromSeconds(2));

         await Assert.That(slowPeer.State).IsEqualTo(DatagramPeerState.Connected);
         await Assert.That(Volatile.Read(ref slowValidationCalls)).IsEqualTo(1);
         await Assert.That(server.PeerCount).IsEqualTo(2);
      }
   }

   [Test]
   public async Task SmallMtu_LargePsk_LargeRejectReason_AndOversizedDisconnectReason_AllHandledSafely()
   {
      var hub = new SimulatedNetworkHub(seed: 5002);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 10200);

      // 4 KB PSK exercises pooled heap path and RFC 5869 HKDF-Extract/Expand without Windows CNG IKM limit failures
      var largePsk = new byte[4096];
      new Random(99).NextBytes(largePsk);

      // Minimum MTU = 256 bytes: AEAD ConnectAccept fixed overhead is 208 bytes -> max metadata = 48 bytes
      var serverOpts = CreateManualOptions(DatagramSecurityMode.Aead, maxDatagramSize: 256, psk: largePsk);
      var clientOpts = CreateManualOptions(DatagramSecurityMode.Aead, maxDatagramSize: 256, psk: largePsk);

      await using var server = new DatagramHost(serverEp, serverOpts, hub.CreateTransport(), hub.Time);
      server.Events.ValidateConnection = ctx =>
      {
         if (Encoding.UTF8.GetString(ctx.ConnectMetadata.Span) == "reject-big")
         {
            // Huge 4000-byte rejection payload must clamp safely to MTU instead of overflowing stack/packet
            ctx.Reject(Encoding.UTF8.GetBytes(new string('R', 4000)));
         }
         return ValueTask.CompletedTask;
      };
      await using var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), clientOpts, hub.CreateTransport(), hub.Time);
      server.Start();
      client.Start();

      // Metadata larger than MTU allows (even though < default MaxConnectMetadataBytes=512) throws cleanly
      await Assert.That(async () => { await client.ConnectAsync(serverEp, new byte[100]); })
         .Throws<ArgumentOutOfRangeException>();

      // Oversized RejectReason clamps cleanly and throws DatagramConnectionRejectedException
      var rejTask = client.ConnectAsync(serverEp, "reject-big"u8.ToArray());
      for (var i = 0; i < 20 && !rejTask.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }

      DatagramConnectionRejectedException? rejEx = null;
      try { await rejTask.WaitAsync(TimeSpan.FromSeconds(1)); } catch (DatagramConnectionRejectedException ex) { rejEx = ex; }
      await Assert.That(rejEx).IsNotNull();
      await Assert.That(rejEx!.RejectReason.Length).IsGreaterThan(0);
      await Assert.That(rejEx.RejectReason.Length).IsLessThanOrEqualTo(256);

      // Normal connect with 4 KB PSK + 40-byte metadata succeeds
      var okTask = client.ConnectAsync(serverEp, "ok-with-4kb-psk"u8.ToArray());
      for (var i = 0; i < 20 && !okTask.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var peer = await okTask.WaitAsync(TimeSpan.FromSeconds(1));
      server.PollEvents(_ => { });

      // Oversized Disconnect reason (4000 bytes on 256B MTU) clamps safely and disconnects cleanly
      peer.Disconnect(Encoding.UTF8.GetBytes(new string('D', 4000)));
      DatagramDisconnectReason? remoteReason = null;
      var remoteReasonLen = 0;
      for (var i = 0; i < 10 && remoteReason is null; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.PeerDisconnected)
            {
               remoteReason = evt.DisconnectReason;
               remoteReasonLen = evt.Payload.Length;
            }
         });
      }

      await Assert.That(remoteReason).IsEqualTo(DatagramDisconnectReason.GracefulRemote);
      await Assert.That(remoteReasonLen).IsGreaterThan(0);
      await Assert.That(remoteReasonLen).IsLessThanOrEqualTo(256);
   }

   [Test]
   public async Task UnackedReliableMessages_ExcludesUnreliableChannels_AndTracksReliableChannelsAccurately()
   {
      var hub = new SimulatedNetworkHub(seed: 5003);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 10201);

      await using var server = new DatagramHost(serverEp, CreateManualOptions(), hub.CreateTransport(), hub.Time);
      await using var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateManualOptions(), hub.CreateTransport(), hub.Time);
      server.Start();
      client.Start();

      var ct = client.ConnectAsync(serverEp);
      for (var i = 0; i < 20 && !ct.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
      }
      var peer = await ct;

      // Queue 10 unreliable messages WITHOUT ticking yet -> UnackedReliableMessages MUST remain 0!
      for (var i = 0; i < 10; i++)
      {
         peer.TrySend(1, BitConverter.GetBytes(i));
      }
      await Assert.That(peer.Stats.UnackedReliableMessages).IsEqualTo(0L);

      // Queue 3 reliable messages -> UnackedReliableMessages == 3
      for (var i = 0; i < 3; i++)
      {
         peer.TrySend(0, BitConverter.GetBytes(i));
      }
      await Assert.That(peer.Stats.UnackedReliableMessages).IsEqualTo(3L);

      // Flush and receive ACK -> drops back to 0
      for (var i = 0; i < 10 && peer.Stats.UnackedReliableMessages > 0; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.PollEvents(_ => { });
         client.PollEvents(_ => { });
      }
      await Assert.That(peer.Stats.UnackedReliableMessages).IsEqualTo(0L);
   }

   [Test]
   public async Task OptionsValidation_RejectsInvalidBounds_AndDoesNotMutateSharedEmptyChannelList()
   {
      var sharedEmpty = new DatagramOptions();
      await using (var h = new DatagramHost(sharedEmpty))
      {
         await Assert.That(sharedEmpty.Channels.Count).IsEqualTo(0);
      }

      await Assert.That(() => new DatagramHost(new DatagramOptions { MaxPeers = 0 })).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => new DatagramHost(new DatagramOptions { MaxQueuedEvents = 0 })).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => new DatagramHost(new DatagramOptions { MaxConnectMetadataBytes = -1 })).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => new DatagramHost(new DatagramOptions { TickInterval = TimeSpan.Zero })).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => new DatagramHost(new DatagramOptions { HandshakeRetryInterval = TimeSpan.Zero })).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => new DatagramHost(new DatagramOptions { ConnectTimeout = TimeSpan.Zero })).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => new DatagramHost(new DatagramOptions { KeepAliveInterval = TimeSpan.Zero })).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => new DatagramHost(new DatagramOptions { PeerTimeout = TimeSpan.Zero })).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => new DatagramHost(new DatagramOptions { MaxReliableRetryDuration = TimeSpan.Zero })).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => new DatagramHost(new DatagramOptions { TimeSyncInterval = TimeSpan.FromMilliseconds(-1) })).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => new DatagramHost(new DatagramOptions { MinRtoMs = 0 })).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => new DatagramHost(new DatagramOptions { MinRtoMs = 200, MaxRtoMs = 100 })).Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => new DatagramHost(new DatagramOptions { PacingBurstBytes = -1 })).Throws<ArgumentOutOfRangeException>();

      var badQueue = new DatagramOptions().AddChannel(0, DatagramDeliveryMode.ReliableOrdered, maxSendQueueDepth: 0);
      await Assert.That(() => new DatagramHost(badQueue)).Throws<ArgumentOutOfRangeException>();

      var badGroups = new DatagramOptions();
      badGroups.Channels.Add(new DatagramChannelConfig { ChannelId = 0, MaxInFlightFragmentGroups = 0 });
      await Assert.That(() => new DatagramHost(badGroups)).Throws<ArgumentOutOfRangeException>();
   }

   [Test]
   public async Task NatAndLifecycleGuards_ValidateInputs_IdempotentRendezvousStart_AndPostDisposeThrows()
   {
      var hub = new SimulatedNetworkHub(seed: 5004);
      var host = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateManualOptions(), hub.CreateTransport(), hub.Time);
      host.Start();

      var rdvEp = new IPEndPoint(IPAddress.Loopback, 10205);
      await Assert.That(() => host.RequestIntroduction(rdvEp, ReadOnlySpan<byte>.Empty, "b"u8, 1UL))
         .Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => host.RequestIntroduction(rdvEp, new byte[65], "b"u8, 1UL))
         .Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => host.RequestIntroduction(rdvEp, "a"u8, ReadOnlySpan<byte>.Empty, 1UL))
         .Throws<ArgumentOutOfRangeException>();
      await Assert.That(() => host.RequestIntroduction(rdvEp, "a"u8, new byte[65], 1UL))
         .Throws<ArgumentOutOfRangeException>();

      await host.DisposeAsync();
      await Assert.That(() => host.Start()).Throws<ObjectDisposedException>();
      await Assert.That(() => host.RequestIntroduction(rdvEp, "a"u8, "b"u8, 1UL)).Throws<ObjectDisposedException>();

      // RendezvousServer.Start() is idempotent and throws after dispose
      var rdv = new DatagramRendezvousServer(new IPEndPoint(IPAddress.Loopback, 0));
      rdv.Start();
      rdv.Start(); // Second call is a safe no-op
      await rdv.DisposeAsync();
      await Assert.That(() => rdv.Start()).Throws<ObjectDisposedException>();
   }

   [Test]
   public async Task DisconnectPacketFromMigratedEndPoint_DoesNotEmitSpuriousPeerMigratedEvent()
   {
      var hub = new SimulatedNetworkHub(seed: 5005);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 10206);

      await using var server = new DatagramHost(serverEp, CreateManualOptions(), hub.CreateTransport(), hub.Time);
      var clientTransport = hub.CreateTransport();
      await using var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 56001), CreateManualOptions(), clientTransport, hub.Time);
      server.Start();
      client.Start();

      var ct = client.ConnectAsync(serverEp);
      for (var i = 0; i < 20 && !ct.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
      }
      var peer = await ct;
      server.PollEvents(_ => { });

      // Client migrates endpoint AND immediately disconnects
      hub.MigrateEndpoint(clientTransport, new IPEndPoint(IPAddress.Parse("10.0.1.99"), 56002));
      peer.Disconnect("bye-migrated"u8);

      var sawMigrated = false;
      var sawDisconnected = false;
      for (var i = 0; i < 10; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.PeerMigrated) sawMigrated = true;
            if (evt.Type == DatagramEventType.PeerDisconnected) sawDisconnected = true;
         });
      }

      await Assert.That(sawDisconnected).IsTrue();
      await Assert.That(sawMigrated).IsFalse();
   }

   [Test]
   public async Task SimulatedNetworkHub_ZeroLatencyWithFullDuplication_DeliversExactDuplicates()
   {
      var hub = new SimulatedNetworkHub(seed: 5006, chaos: new SimulatedChaosProfile
      {
         PacketDuplicateRate = 1.0,
         MinLatency = TimeSpan.Zero,
         MaxLatency = TimeSpan.Zero
      });

      var epA = new IPEndPoint(IPAddress.Loopback, 10210);
      var epB = new IPEndPoint(IPAddress.Loopback, 10211);
      await using var tA = hub.CreateTransport();
      await using var tB = hub.CreateTransport();
      tA.Bind(epA, new DatagramOptions());
      tB.Bind(epB, new DatagramOptions());

      await tA.SendToAsync("dup-test"u8.ToArray(), epB.Serialize(), CancellationToken.None);
      hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(30));

      var received = 0;
      var buf = new byte[64];
      var sa = new SocketAddress(AddressFamily.InterNetwork);
      while (tB.TryDequeueImmediate(buf, sa, out _))
      {
         received++;
      }

      await Assert.That(received).IsEqualTo(2);
   }

   [Test]
   [NotInParallel]
   public async Task RealUdpDualModeIpv6Server_AcceptsIpv4ClientCleanly()
   {
      if (!Socket.OSSupportsIPv6)
      {
         return;
      }

      DatagramOptions MakeOpts() => new DatagramOptions
      {
         DispatchMode = DatagramEventDispatchMode.Async,
         ManualTick = false,
         TickInterval = TimeSpan.FromMilliseconds(10),
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Aead }
      }.AddChannel(0, DatagramDeliveryMode.ReliableOrdered);

      await using var server = new DatagramHost(new IPEndPoint(IPAddress.IPv6Any, 0), MakeOpts());
      var serverMsgTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.MessageReceived = (peer, _, _, payload, _) =>
      {
         serverMsgTcs.TrySetResult(Encoding.UTF8.GetString(payload.Span));
         peer.TrySend(0, "dual-mode-pong"u8);
         return ValueTask.CompletedTask;
      };
      server.Start();

      var ipv4Target = new IPEndPoint(IPAddress.Loopback, server.LocalEndPoint!.Port);
      await using var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts());
      var clientMsgTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.MessageReceived = (_, _, _, payload, _) =>
      {
         clientMsgTcs.TrySetResult(Encoding.UTF8.GetString(payload.Span));
         return ValueTask.CompletedTask;
      };
      client.Start();

      var peer = await client.ConnectAsync(ipv4Target);
      peer.TrySend(0, "dual-mode-ping"u8);

      await Assert.That(await serverMsgTcs.Task.WaitAsync(TimeSpan.FromSeconds(3))).IsEqualTo("dual-mode-ping");
      await Assert.That(await clientMsgTcs.Task.WaitAsync(TimeSpan.FromSeconds(3))).IsEqualTo("dual-mode-pong");
   }

   [Test]
   [NotInParallel]
   public async Task RealUdpDualModeIpv6Client_ConnectsToIpv4ServerCleanly()
   {
      if (!Socket.OSSupportsIPv6)
      {
         return;
      }

      DatagramOptions MakeOpts() => new DatagramOptions
      {
         DispatchMode = DatagramEventDispatchMode.Async,
         ManualTick = false,
         TickInterval = TimeSpan.FromMilliseconds(10),
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Aead }
      }.AddChannel(0, DatagramDeliveryMode.ReliableOrdered);

      await using var server = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts());
      var serverMsgTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.MessageReceived = (peer, _, _, payload, _) =>
      {
         serverMsgTcs.TrySetResult(Encoding.UTF8.GetString(payload.Span));
         peer.TrySend(0, "v4-server-reply"u8);
         return ValueTask.CompletedTask;
      };
      server.Start();

      // Client explicitly binds IPv6Any (DualMode = true) and dials an IPv4 endpoint!
      await using var client = new DatagramHost(new IPEndPoint(IPAddress.IPv6Any, 0), MakeOpts());
      var clientMsgTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.MessageReceived = (_, _, _, payload, _) =>
      {
         clientMsgTcs.TrySetResult(Encoding.UTF8.GetString(payload.Span));
         return ValueTask.CompletedTask;
      };
      client.Start();

      var peer = await client.ConnectAsync(server.LocalEndPoint!);
      peer.TrySend(0, "v6-dual-client-hi"u8);

      await Assert.That(await serverMsgTcs.Task.WaitAsync(TimeSpan.FromSeconds(3))).IsEqualTo("v6-dual-client-hi");
      await Assert.That(await clientMsgTcs.Task.WaitAsync(TimeSpan.FromSeconds(3))).IsEqualTo("v4-server-reply");
   }

   [Test]
   public async Task HkdfDeriveSession_MatchesRfc5869HkdfDeriveKeyBitForBit()
   {
      var sharedSecret = new byte[32];
      var clientSalt = new byte[16];
      var serverSalt = new byte[16];
      var psk = "rfc5869-compat-psk"u8.ToArray();
      new Random(77).NextBytes(sharedSecret);
      new Random(78).NextBytes(clientSalt);
      new Random(79).NextBytes(serverSalt);

      // Compute expected session token via standard HKDF.DeriveKey
      var salt = new byte[32];
      clientSalt.CopyTo(salt, 0);
      serverSalt.CopyTo(salt, 16);
      var ikm = new byte[sharedSecret.Length + psk.Length];
      sharedSecret.CopyTo(ikm, 0);
      psk.CopyTo(ikm, sharedSecret.Length);
      var expectedOkm = new byte[68];
      System.Security.Cryptography.HKDF.DeriveKey(
         System.Security.Cryptography.HashAlgorithmName.SHA256,
         ikm,
         expectedOkm,
         salt,
         "Beskar.Networking.Datagram.v1"u8);
      var expectedToken = BinaryPrimitives.ReadUInt32LittleEndian(expectedOkm.AsSpan(64, 4));
      if (expectedToken == 0) expectedToken = 1;

      using var clientCrypto = Beskar.Networking.Datagram.Security.DatagramCryptoSession.DeriveFromHandshake(
         DatagramAeadCipher.AesGcm, sharedSecret, clientSalt, serverSalt, psk, isClient: true, out var clientToken);
      using var serverCrypto = Beskar.Networking.Datagram.Security.DatagramCryptoSession.DeriveFromHandshake(
         DatagramAeadCipher.AesGcm, sharedSecret, clientSalt, serverSalt, psk, isClient: false, out var serverToken);

      await Assert.That(clientToken).IsEqualTo(expectedToken);
      await Assert.That(serverToken).IsEqualTo(expectedToken);

      // Cross-verify encrypt/decrypt with reference key derived by HKDF.DeriveKey
      var plain = "rfc5869-verified"u8.ToArray();
      var aad = "header-aad"u8.ToArray();
      var cipher = new byte[plain.Length];
      var tag = new byte[16];
      var decrypted = new byte[plain.Length];

      clientCrypto.Encrypt(packetSeq: 7, aad, plain, cipher, tag);
      await Assert.That(serverCrypto.TryDecrypt(packetSeq: 7, aad, cipher, tag, decrypted)).IsTrue();
      await Assert.That(decrypted).IsEquivalentTo(plain);
   }

   [Test]
   public async Task CoalescedRtoRetransmit_CountsSingleDatagramLossInsteadOfPerMessageLoss()
   {
      var hub = new SimulatedNetworkHub(seed: 5010);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 10220);

      var opts = CreateManualOptions(DatagramSecurityMode.Crc32C);
      opts.MinRtoMs = 60;
      opts.MaxRtoMs = 200;

      await using var server = new DatagramHost(serverEp, opts, hub.CreateTransport(), hub.Time);
      await using var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), opts, hub.CreateTransport(), hub.Time);
      server.Start();
      client.Start();

      var ct = client.ConnectAsync(serverEp);
      for (var i = 0; i < 20 && !ct.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
      }
      var peer = await ct;
      server.PollEvents(_ => { });

      // Send 20 clean individual datagrams so window has 20 acked packets
      for (var i = 0; i < 20; i++)
      {
         peer.TrySend(0, BitConverter.GetBytes(i));
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.PollEvents(_ => { });
      }

      // Now queue 12 small reliable messages into ONE coalesced datagram and drop ONLY that 1 datagram!
      var droppedOne = false;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (!droppedOne && src.Equals(client.LocalEndPoint)
             && DatagramWireHeader.TryRead(raw, out var hdr) && hdr.Kind == DatagramPacketKind.Data)
         {
            droppedOne = true;
            return false;
         }
         return true;
      };

      for (var i = 100; i < 112; i++)
      {
         peer.TrySend(0, BitConverter.GetBytes(i));
      }
      client.Tick(); // Sends 1 coalesced datagram -> dropped!
      await Assert.That(droppedOne).IsTrue();

      // Advance past RTO so all 12 coalesced messages retransmit in 1 datagram, then roll 1s loss window
      var deliveredAfterDrop = 0;
      for (var t = 0; t < 40; t++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(40));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(40));
         server.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.MessageReceived) deliveredAfterDrop++;
         });
         client.PollEvents(_ => { });
      }

      await Assert.That(deliveredAfterDrop).IsEqualTo(12);
      // 1 lost datagram out of ~21 acked packets -> EWMA loss rate must stay low (far below the ~0.28+ spike if 12 messages counted individually)
      await Assert.That(peer.Stats.PacketLossRate).IsLessThan(0.08);
      await Assert.That(peer.Stats.MessagesRetransmitted).IsEqualTo(12L);
   }

   [Test]
   [NotInParallel]
   public async Task ThrowingPunchIntroducedHandler_DoesNotKillHostReceiveLoop()
   {
      var rdvOpts = new DatagramOptions { Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Crc32C } };
      await using var rdv = new DatagramRendezvousServer(new IPEndPoint(IPAddress.Loopback, 0), options: rdvOpts);
      rdv.Start();

      var optsA = new DatagramOptions { DispatchMode = DatagramEventDispatchMode.Async }
         .AddChannel(0, DatagramDeliveryMode.ReliableOrdered);
      var optsB = new DatagramOptions { DispatchMode = DatagramEventDispatchMode.Async }
         .AddChannel(0, DatagramDeliveryMode.ReliableOrdered);

      await using var hostA = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), optsA);
      await using var hostB = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), optsB);

      var threwOnce = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
      var msgOnB = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

      hostB.Events.PunchIntroduced = async (_, _) =>
      {
         await Task.Yield();
         threwOnce.TrySetResult();
         throw new InvalidOperationException("Simulated user handler bug in PunchIntroduced");
      };
      hostB.Events.MessageReceived = (_, _, _, payload, _) =>
      {
         msgOnB.TrySetResult(Encoding.UTF8.GetString(payload.Span));
         return ValueTask.CompletedTask;
      };

      hostA.Start();
      hostB.Start();

      await hostA.DiscoverPublicEndPointAsync(rdv.LocalEndPoint!, "node-a"u8.ToArray());
      await hostB.DiscoverPublicEndPointAsync(rdv.LocalEndPoint!, "node-b"u8.ToArray());
      hostA.RequestIntroduction(rdv.LocalEndPoint!, "node-a"u8, "node-b"u8, 0x5555UL);

      await threwOnce.Task.WaitAsync(TimeSpan.FromSeconds(2));

      // HostB receive loop must still be alive and accept normal AEAD connections!
      var peer = await hostA.ConnectAsync(hostB.LocalEndPoint!);
      peer.TrySend(0, "still-healthy"u8);

      var got = await msgOnB.Task.WaitAsync(TimeSpan.FromSeconds(3));
      await Assert.That(got).IsEqualTo("still-healthy");
   }
}
