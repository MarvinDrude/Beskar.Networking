using System.Net;
using System.Text;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Protocol;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Networking.Datagram.Tests;

public class HandshakeSecurityAndAdversarialTests
{
   private static DatagramOptions CreateOptions(
      DatagramSecurityMode mode = DatagramSecurityMode.Aead,
      DatagramAeadCipher cipher = DatagramAeadCipher.Auto,
      byte[]? psk = null,
      bool allowMigration = true,
      int maxPeers = 128)
   {
      return new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxPeers = maxPeers,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero,
         ConnectTimeout = TimeSpan.FromSeconds(5),
         HandshakeRetryInterval = TimeSpan.FromMilliseconds(100),
         Security = new DatagramSecurityOptions
         {
            Mode = mode,
            AeadCipher = cipher,
            PreSharedKey = psk,
            PadConnectRequests = true,
            AllowEndpointMigration = allowMigration
         }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered)
      .AddChannel(1, DatagramDeliveryMode.UnreliableSequenced);
   }

   [Test]
   public async Task AntiAmplification_ConnectRequestIsPaddedAndChallengeIsMuchSmaller()
   {
      var hub = new SimulatedNetworkHub(seed: 601);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9701);

      var server = new DatagramHost(serverEp, CreateOptions(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateOptions(), hub.CreateTransport(), hub.Time);
      server.Start();
      client.Start();

      var reqSize = 0;
      var challengeSize = 0;
      hub.OnPacketSending = (_, _, raw) =>
      {
         if (DatagramWireHeader.TryRead(raw, out var hdr))
         {
            if (hdr.Kind == DatagramPacketKind.ConnectRequest) reqSize = raw.Length;
            if (hdr.Kind == DatagramPacketKind.ConnectChallenge) challengeSize = raw.Length;
         }
         return true;
      };

      var task = client.ConnectAsync(serverEp);
      for (var i = 0; i < 20 && !task.IsCompleted; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }

      await task;
      await Assert.That(reqSize).IsEqualTo(1200);
      await Assert.That(challengeSize).IsGreaterThan(0);
      await Assert.That(challengeSize).IsLessThan(250);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task HandshakeDropRecovery_RecoversWhenEveryIndividualHandshakeStepDropsTwiceWithoutDuplicatePeers()
   {
      var kindsToDrop = new[]
      {
         DatagramPacketKind.ConnectRequest,
         DatagramPacketKind.ConnectChallenge,
         DatagramPacketKind.ConnectAccept,
         DatagramPacketKind.ConnectAck
      };

      foreach (var kind in kindsToDrop)
      {
         var hub = new SimulatedNetworkHub(seed: 602 + (int)kind);
         var serverEp = new IPEndPoint(IPAddress.Loopback, 9710 + (int)kind);

         var server = new DatagramHost(serverEp, CreateOptions(), hub.CreateTransport(), hub.Time);
         var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateOptions(), hub.CreateTransport(), hub.Time);
         server.Start();
         client.Start();

         var dropped = 0;
         hub.OnPacketSending = (_, _, raw) =>
         {
            if (DatagramWireHeader.TryRead(raw, out var hdr) && hdr.Kind == kind && dropped < 2)
            {
               dropped++;
               return false;
            }
            return true;
         };

         var connectTask = client.ConnectAsync(serverEp, "retry-player"u8.ToArray());
         for (var i = 0; i < 60 && !connectTask.IsCompleted; i++)
         {
            client.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(35));
            server.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(35));
         }

         var clientPeer = await connectTask;
         await Assert.That(dropped).IsEqualTo(2);
         await Assert.That(server.PeerCount).IsEqualTo(1);
         await Assert.That(clientPeer.State).IsEqualTo(DatagramPeerState.Connected);

         // Confirm post-handshake encrypted data flows cleanly
         clientPeer.TrySend(0, "post-retry-ok"u8);
         string? got = null;
         for (var i = 0; i < 15; i++)
         {
            client.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            server.Tick();
            server.PollEvents(evt =>
            {
               if (evt.Type == DatagramEventType.MessageReceived)
                  got = Encoding.UTF8.GetString(evt.Payload.Span);
            });
         }
         await Assert.That(got).IsEqualTo("post-retry-ok");

         await client.DisposeAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task AeadTamperDetection_RejectsHeaderAadBodyAndTagFlips_ForBothAesGcmAndChaCha20()
   {
      var ciphers = new[] { DatagramAeadCipher.AesGcm, DatagramAeadCipher.ChaCha20Poly1305 };

      foreach (var cipher in ciphers)
      {
         var hub = new SimulatedNetworkHub(seed: 603 + (int)cipher);
         var serverEp = new IPEndPoint(IPAddress.Loopback, 9720 + (int)cipher);

         var server = new DatagramHost(serverEp, CreateOptions(DatagramSecurityMode.Aead, cipher), hub.CreateTransport(), hub.Time);
         var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateOptions(DatagramSecurityMode.Aead, cipher), hub.CreateTransport(), hub.Time);
         server.Start();
         client.Start();

         var connectTask = client.ConnectAsync(serverEp);
         for (var i = 0; i < 20 && !connectTask.IsCompleted; i++)
         {
            client.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            server.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         }
         var peer = await connectTask;
         server.PollEvents(_ => { });

         // Capture one genuine encrypted Data packet
         byte[]? captured = null;
         IPEndPoint? clientEp = null;
         hub.OnPacketSending = (src, _, raw) =>
         {
            if (src.Equals(client.LocalEndPoint) && DatagramWireHeader.TryRead(raw, out var hdr) && hdr.Kind == DatagramPacketKind.Data)
            {
               captured = (byte[])raw.Clone();
               clientEp = src;
               return false;
            }
            return true;
         };

         peer.TrySend(1, "secret-payload-xyz"u8);
         client.Tick();
         hub.OnPacketSending = null;
         await Assert.That(captured).IsNotNull();

         // 1. Flip a bit in the 21-byte AAD wire header (AckSeq byte 10)
         var aadTampered = (byte[])captured!.Clone();
         aadTampered[10] ^= 0x01;
         hub.InjectRawPacket(clientEp!, serverEp, aadTampered);

         // 2. Flip a bit in the encrypted body
         var bodyTampered = (byte[])captured.Clone();
         bodyTampered[DatagramWireHeader.Size + 2] ^= 0x80;
         hub.InjectRawPacket(clientEp!, serverEp, bodyTampered);

         // 3. Flip a bit in the 16-byte AEAD tag
         var tagTampered = (byte[])captured.Clone();
         tagTampered[^1] ^= 0x01;
         hub.InjectRawPacket(clientEp!, serverEp, tagTampered);

         var deliveredCount = 0;
         server.Tick();
         server.PollEvents(evt => { if (evt.Type == DatagramEventType.MessageReceived) deliveredCount++; });
         await Assert.That(deliveredCount).IsEqualTo(0);

         // 4. Original untampered packet still authenticates and delivers cleanly!
         hub.InjectRawPacket(clientEp!, serverEp, captured);
         string? cleanMsg = null;
         server.Tick();
         server.PollEvents(evt => { if (evt.Type == DatagramEventType.MessageReceived) cleanMsg = Encoding.UTF8.GetString(evt.Payload.Span); });
         await Assert.That(cleanMsg).IsEqualTo("secret-payload-xyz");

         await client.DisposeAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task ReplayedAeadPacketFromSpoofedIp_CannotHijackEndpointMigrationOrRedeliver()
   {
      var hub = new SimulatedNetworkHub(seed: 604);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9730);
      var originalClientEp = new IPEndPoint(IPAddress.Loopback, 51001);
      var attackerEp = new IPEndPoint(IPAddress.Parse("198.51.100.99"), 6666);

      var server = new DatagramHost(serverEp, CreateOptions(allowMigration: true), hub.CreateTransport(), hub.Time);
      var clientTransport = hub.CreateTransport();
      var client = new DatagramHost(originalClientEp, CreateOptions(allowMigration: true), clientTransport, hub.Time);
      server.Start();
      client.Start();

      var connectTask = client.ConnectAsync(serverEp);
      for (var i = 0; i < 20 && !connectTask.IsCompleted; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var clientPeer = await connectTask;
      server.PollEvents(_ => { });

      // Record a genuine AEAD Data packet while allowing it to be delivered normally
      byte[]? sniffedPacket = null;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (src.Equals(originalClientEp) && DatagramWireHeader.TryRead(raw, out var hdr) && hdr.Kind == DatagramPacketKind.Data)
         {
            sniffedPacket = (byte[])raw.Clone();
         }
         return true;
      };

      clientPeer.TrySend(1, "legit-1"u8);
      client.Tick();
      server.Tick();
      hub.OnPacketSending = null;

      var delivered = new List<string>();
      var migrations = 0;
      server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived) delivered.Add(Encoding.UTF8.GetString(evt.Payload.Span));
         if (evt.Type == DatagramEventType.PeerMigrated) migrations++;
      });

      await Assert.That(delivered).IsEquivalentTo(["legit-1"]);
      await Assert.That(sniffedPacket).IsNotNull();

      // Attacker replays exact sniffed packet from a spoofed IP!
      hub.InjectRawPacket(attackerEp, serverEp, sniffedPacket!);
      server.Tick();
      server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived) delivered.Add(Encoding.UTF8.GetString(evt.Payload.Span));
         if (evt.Type == DatagramEventType.PeerMigrated) migrations++;
      });

      // MUST NOT deliver duplicate AND MUST NOT migrate endpoint to attackerEp!
      await Assert.That(delivered.Count).IsEqualTo(1);
      await Assert.That(migrations).IsEqualTo(0);
      server.TryGetPeer(1, out var serverPeer);
      await Assert.That(serverPeer!.RemoteEndPoint).IsEqualTo(originalClientEp);

      // Now test AllowEndpointMigration = false on a fresh pair
      await client.DisposeAsync();
      await server.DisposeAsync();

      var serverNoMigEp = new IPEndPoint(IPAddress.Loopback, 9731);
      var serverNoMig = new DatagramHost(serverNoMigEp, CreateOptions(allowMigration: false), hub.CreateTransport(), hub.Time);
      var ct2 = hub.CreateTransport();
      var client2 = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 51002), CreateOptions(allowMigration: false), ct2, hub.Time);
      serverNoMig.Start();
      client2.Start();

      var t2 = client2.ConnectAsync(serverNoMigEp);
      for (var i = 0; i < 20 && !t2.IsCompleted; i++)
      {
         client2.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         serverNoMig.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var p2 = await t2;
      serverNoMig.PollEvents(_ => { });

      hub.MigrateEndpoint(ct2, new IPEndPoint(IPAddress.Parse("10.1.2.3"), 55551));
      p2.TrySend(0, "should-drop"u8);
      client2.Tick();
      serverNoMig.Tick();

      var gotAfterDisabledMig = 0;
      serverNoMig.PollEvents(evt => { if (evt.Type is DatagramEventType.MessageReceived or DatagramEventType.PeerMigrated) gotAfterDisabledMig++; });
      await Assert.That(gotAfterDisabledMig).IsEqualTo(0);

      await client2.DisposeAsync();
      await serverNoMig.DisposeAsync();
   }

   [Test]
   public async Task DelayedOldEndPointPacket_DoesNotYankAlreadyMigratedPeerBack()
   {
      var hub = new SimulatedNetworkHub(seed: 605);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9732);
      var oldEp = new IPEndPoint(IPAddress.Loopback, 51010);
      var newEp = new IPEndPoint(IPAddress.Parse("10.9.8.7"), 51011);

      var server = new DatagramHost(serverEp, CreateOptions(allowMigration: true), hub.CreateTransport(), hub.Time);
      var ct = hub.CreateTransport();
      var client = new DatagramHost(oldEp, CreateOptions(allowMigration: true), ct, hub.Time);
      server.Start();
      client.Start();

      var t = client.ConnectAsync(serverEp);
      for (var i = 0; i < 20 && !t.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var peer = await t;
      server.PollEvents(_ => { });

      // Hold a packet sent from oldEp (PacketSeq = 2)
      byte[]? delayedOldPkt = null;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (src.Equals(oldEp) && DatagramWireHeader.TryRead(raw, out var hdr) && hdr.Kind == DatagramPacketKind.Data)
         {
            delayedOldPkt = (byte[])raw.Clone();
            return false;
         }
         return true;
      };
      peer.TrySend(1, "from-old"u8);
      client.Tick();
      hub.OnPacketSending = null;

      // Migrate client to newEp and send a newer packet (PacketSeq = 3)
      hub.MigrateEndpoint(ct, newEp);
      peer.TrySend(1, "from-new"u8);
      client.Tick();
      server.Tick();

      server.TryGetPeer(1, out var serverPeer);
      await Assert.That(serverPeer!.RemoteEndPoint).IsEqualTo(newEp);
      server.PollEvents(_ => { });

      // Now delayed PacketSeq=2 from oldEp arrives out of order -> must NOT migrate back to oldEp!
      hub.InjectRawPacket(oldEp, serverEp, delayedOldPkt!);
      server.Tick();

      var bouncedBack = false;
      server.PollEvents(evt => { if (evt.Type == DatagramEventType.PeerMigrated) bouncedBack = true; });
      await Assert.That(bouncedBack).IsFalse();
      await Assert.That(serverPeer.RemoteEndPoint).IsEqualTo(newEp);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ChannelMismatch_MaxPeers_ExpiredCookie_AndEcdhKeyRotation_AllBehaveCorrectly()
   {
      // 1. Mismatched ChannelConfigHash -> explicit DatagramConnectionRejectedException
      {
         var hub = new SimulatedNetworkHub(seed: 606);
         var serverEp = new IPEndPoint(IPAddress.Loopback, 9740);
         var serverOpts = CreateOptions();
         var clientOpts = CreateOptions().AddChannel(9, DatagramDeliveryMode.ReliableOrdered); // extra mismatch channel

         var server = new DatagramHost(serverEp, serverOpts, hub.CreateTransport(), hub.Time);
         var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), clientOpts, hub.CreateTransport(), hub.Time);
         server.Start();
         client.Start();

         var connectTask = client.ConnectAsync(serverEp);
         for (var i = 0; i < 30 && !connectTask.IsCompleted; i++)
         {
            client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         }

         DatagramConnectionRejectedException? mismatchEx = null;
         try { await connectTask; } catch (DatagramConnectionRejectedException ex) { mismatchEx = ex; }
         await Assert.That(mismatchEx).IsNotNull();
         await Assert.That(mismatchEx!.Message).Contains("Channel configuration mismatch");

         await client.DisposeAsync();
         await server.DisposeAsync();
      }

      // 2. MaxPeers = 1 rejects 2nd client with explicit DatagramConnectionRejectedException
      {
         var hub = new SimulatedNetworkHub(seed: 607);
         var serverEp = new IPEndPoint(IPAddress.Loopback, 9741);
         var server = new DatagramHost(serverEp, CreateOptions(maxPeers: 1), hub.CreateTransport(), hub.Time);
         var c1 = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateOptions(), hub.CreateTransport(), hub.Time);
         var c2 = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateOptions(), hub.CreateTransport(), hub.Time);
         server.Start(); c1.Start(); c2.Start();

         var t1 = c1.ConnectAsync(serverEp);
         for (var i = 0; i < 20 && !t1.IsCompleted; i++)
         {
            c1.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         }
         await t1;

         var t2 = c2.ConnectAsync(serverEp);
         for (var i = 0; i < 20 && !t2.IsCompleted; i++)
         {
            c2.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         }

         DatagramConnectionRejectedException? fullEx = null;
         try { await t2; } catch (DatagramConnectionRejectedException ex) { fullEx = ex; }
         await Assert.That(fullEx).IsNotNull();
         await Assert.That(fullEx!.Message).Contains("peer limit");
         await Assert.That(server.PeerCount).IsEqualTo(1);

         await c1.DisposeAsync();
         await c2.DisposeAsync();
         await server.DisposeAsync();
      }

      // 3. Expired cookie (> ChallengeCookieLifetime) rejected cleanly; AND server ECDH key rotation mid-handshake works!
      {
         var hub = new SimulatedNetworkHub(seed: 608);
         var serverEp = new IPEndPoint(IPAddress.Loopback, 9742);
         var serverOpts = CreateOptions();
         serverOpts.Security.ChallengeCookieLifetime = TimeSpan.FromSeconds(120); // allow >60s rotation test!
         var clientOpts = CreateOptions();
         clientOpts.ConnectTimeout = TimeSpan.FromSeconds(150);

         var server = new DatagramHost(serverEp, serverOpts, hub.CreateTransport(), hub.Time);
         var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), clientOpts, hub.CreateTransport(), hub.Time);
         server.Start();
         client.Start();

         // Hold ConnectAccept while advancing clock 65s to trigger server ECDH key rotation (_currentKey -> _previousKey)
         byte[]? heldAccept = null;
         IPEndPoint? clientEp = null;
         hub.OnPacketSending = (src, _, raw) =>
         {
            if (DatagramWireHeader.TryRead(raw, out var hdr) && hdr.Kind == DatagramPacketKind.ConnectAccept)
            {
               heldAccept = (byte[])raw.Clone();
               clientEp = src;
               return false;
            }
            return true;
         };

         var connectTask = client.ConnectAsync(serverEp);
         for (var i = 0; i < 15 && heldAccept is null; i++)
         {
            client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         }
         await Assert.That(heldAccept).IsNotNull();

         // Trigger server ephemeral key rotation by advancing 65s and triggering a dummy ConnectRequest from another IP
         hub.AdvanceAndDeliver(TimeSpan.FromSeconds(65));
         var probeClient = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), clientOpts, hub.CreateTransport(), hub.Time);
         probeClient.Start();
         _ = probeClient.ConnectAsync(serverEp);
         server.Tick();

         // Now release original ConnectAccept referencing _previousKey -> must still derive shared secret & connect!
         hub.OnPacketSending = null;
         hub.InjectRawPacket(clientEp!, serverEp, heldAccept!);
         for (var i = 0; i < 10 && !connectTask.IsCompleted; i++)
         {
            server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(10));
            client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(10));
         }

         var connectedPeer = await connectTask;
         await Assert.That(connectedPeer.State).IsEqualTo(DatagramPeerState.Connected);

         await probeClient.DisposeAsync();
         await client.DisposeAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task SecurityModeNoneAndCrc32C_EndToEndAndCorruptionRejection()
   {
      // SecurityMode.None end-to-end
      {
         var hub = new SimulatedNetworkHub(seed: 609);
         var ep = new IPEndPoint(IPAddress.Loopback, 9750);
         var server = new DatagramHost(ep, CreateOptions(DatagramSecurityMode.None), hub.CreateTransport(), hub.Time);
         var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateOptions(DatagramSecurityMode.None), hub.CreateTransport(), hub.Time);
         server.Start(); client.Start();

         var t = client.ConnectAsync(ep);
         for (var i = 0; i < 15 && !t.IsCompleted; i++)
         {
            client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         }
         var peer = await t;
         peer.TrySend(0, "plaintext-none"u8);

         string? got = null;
         for (var i = 0; i < 10; i++)
         {
            client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            server.Tick();
            server.PollEvents(evt => { if (evt.Type == DatagramEventType.MessageReceived) got = Encoding.UTF8.GetString(evt.Payload.Span); });
         }
         await Assert.That(got).IsEqualTo("plaintext-none");

         await client.DisposeAsync();
         await server.DisposeAsync();
      }

      // SecurityMode.Crc32C rejects 1-bit flip in body
      {
         var hub = new SimulatedNetworkHub(seed: 610);
         var ep = new IPEndPoint(IPAddress.Loopback, 9751);
         var server = new DatagramHost(ep, CreateOptions(DatagramSecurityMode.Crc32C), hub.CreateTransport(), hub.Time);
         var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateOptions(DatagramSecurityMode.Crc32C), hub.CreateTransport(), hub.Time);
         server.Start(); client.Start();

         var t = client.ConnectAsync(ep);
         for (var i = 0; i < 15 && !t.IsCompleted; i++)
         {
            client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         }
         var peer = await t;
         server.PollEvents(_ => { });

         hub.OnPacketSending = (src, _, raw) =>
         {
            if (src.Equals(client.LocalEndPoint) && DatagramWireHeader.TryRead(raw, out var hdr) && hdr.Kind == DatagramPacketKind.Data)
            {
               raw[^1] ^= 0xFF; // corrupt CRC trailer
            }
            return true;
         };

         peer.TrySend(1, "corrupted-crc"u8);
         client.Tick();
         server.Tick();

         var received = 0;
         server.PollEvents(evt => { if (evt.Type == DatagramEventType.MessageReceived) received++; });
         await Assert.That(received).IsEqualTo(0);

         await client.DisposeAsync();
         await server.DisposeAsync();
      }
   }

   [Test]
   public async Task ExpiredCookie_AndStolenCookieFromWrongIp_AreRejected()
   {
      var hub = new SimulatedNetworkHub(seed: 611);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9760);
      var serverOpts = CreateOptions();
      serverOpts.Security.ChallengeCookieLifetime = TimeSpan.FromMilliseconds(400);

      var clientOpts = CreateOptions();
      clientOpts.ConnectTimeout = TimeSpan.FromSeconds(5);

      var server = new DatagramHost(serverEp, serverOpts, hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 53001), clientOpts, hub.CreateTransport(), hub.Time);
      server.Start();
      client.Start();

      // Capture ConnectAccept before it reaches server
      byte[]? stolenAccept = null;
      IPEndPoint? realClientEp = null;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (DatagramWireHeader.TryRead(raw, out var hdr) && hdr.Kind == DatagramPacketKind.ConnectAccept)
         {
            stolenAccept = (byte[])raw.Clone();
            realClientEp = src;
            return false;
         }
         return true;
      };

      using var cts = new CancellationTokenSource();
      var connectTask = client.ConnectAsync(serverEp, ct: cts.Token);
      for (var i = 0; i < 10 && stolenAccept is null; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
      }
      await Assert.That(stolenAccept).IsNotNull();

      // 1. Replay stolen ConnectAccept immediately from a DIFFERENT IP -> EndPointHash mismatch -> rejected!
      hub.InjectRawPacket(new IPEndPoint(IPAddress.Parse("198.51.100.77"), 9999), serverEp, stolenAccept!);
      server.Tick();
      await Assert.That(server.PeerCount).IsEqualTo(0);

      // 2. Advance past ChallengeCookieLifetime (400ms -> advance 500ms) and replay from ORIGINAL IP -> expired -> rejected!
      hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(500));
      hub.InjectRawPacket(realClientEp!, serverEp, stolenAccept!);
      server.Tick();
      await Assert.That(server.PeerCount).IsEqualTo(0);

      // 3. CancellationToken cleanly cancels in-progress ConnectAsync
      await cts.CancelAsync();
      var cancelled = false;
      try { await connectTask; } catch (OperationCanceledException) { cancelled = true; }
      await Assert.That(cancelled).IsTrue();

      // 4. Oversized ConnectMetadata throws ArgumentOutOfRangeException
      var oversized = new byte[clientOpts.MaxConnectMetadataBytes + 1];
      await Assert.That(async () => { await client.ConnectAsync(serverEp, oversized); }).Throws<ArgumentOutOfRangeException>();

      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}
