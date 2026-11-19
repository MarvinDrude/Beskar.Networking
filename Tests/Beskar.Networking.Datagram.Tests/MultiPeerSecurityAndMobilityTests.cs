using System.Net;
using System.Text;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Networking.Datagram.Tests;

public class MultiPeerSecurityAndMobilityTests
{
   private static DatagramOptions CreateSecureOptions(byte[]? psk = null)
   {
      return new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxBytesPerSecondPerPeer = null,
         PeerTimeout = TimeSpan.FromSeconds(3),
         TimeSyncInterval = TimeSpan.Zero,
         Security = new DatagramSecurityOptions
         {
            Mode = DatagramSecurityMode.Aead,
            PreSharedKey = psk,
            PadConnectRequests = true,
            AllowEndpointMigration = true
         }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered)
      .AddChannel(1, DatagramDeliveryMode.UnreliableSequenced);
   }

   [Test]
   public async Task MultiSenderServer_IdentifiesEachSenderValidatesMetadataAndBroadcasts()
   {
      var hub = new SimulatedNetworkHub(seed: 1);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9100);
      var psk = "secret-lobby-key"u8.ToArray();

      var server = new DatagramHost(serverEp, CreateSecureOptions(psk), hub.CreateTransport(), hub.Time);
      server.Events.ValidateConnection = ctx =>
      {
         var name = Encoding.UTF8.GetString(ctx.ConnectMetadata.Span);
         if (name == "banned-player")
         {
            ctx.Reject("Player is banned"u8);
         }
         else
         {
            ctx.PeerTag = name;
         }
         return ValueTask.CompletedTask;
      };
      server.Start();

      var c1 = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateSecureOptions(psk), hub.CreateTransport(), hub.Time);
      var c2 = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateSecureOptions(psk), hub.CreateTransport(), hub.Time);
      var cBanned = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateSecureOptions(psk), hub.CreateTransport(), hub.Time);

      var t1 = c1.ConnectAsync(serverEp, "player-one"u8.ToArray());
      var t2 = c2.ConnectAsync(serverEp, "player-two"u8.ToArray());
      var tBanned = cBanned.ConnectAsync(serverEp, "banned-player"u8.ToArray());

      for (var i = 0; i < 40 && (!t1.IsCompleted || !t2.IsCompleted || !tBanned.IsCompleted); i++)
      {
         c1.Tick(); c2.Tick(); cBanned.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }

      var p1 = await t1;
      var p2 = await t2;

      DatagramConnectionRejectedException? rejectedEx = null;
      try
      {
         await tBanned;
      }
      catch (DatagramConnectionRejectedException ex)
      {
         rejectedEx = ex;
      }

      await Assert.That(rejectedEx).IsNotNull();
      await Assert.That(Encoding.UTF8.GetString(rejectedEx!.RejectReason.Span)).IsEqualTo("Player is banned");
      await Assert.That(server.PeerCount).IsEqualTo(2);

      // Both players send to server; server checks Peer.PeerId and Peer.Tag
      p1.TrySend(0, "roll:6"u8);
      p2.TrySend(0, "move:right"u8);

      var serverObserved = new Dictionary<string, string>();
      DatagramPeer? serverSideP1 = null;

      for (var i = 0; i < 20; i++)
      {
         c1.Tick(); c2.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));

         server.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.MessageReceived)
            {
               var tag = (string)evt.Peer.Tag!;
               serverObserved[tag] = Encoding.UTF8.GetString(evt.Payload.Span);
               if (tag == "player-one") serverSideP1 = evt.Peer;
            }
         });
      }

      await Assert.That(serverObserved["player-one"]).IsEqualTo("roll:6");
      await Assert.That(serverObserved["player-two"]).IsEqualTo("move:right");

      // Server broadcasts excluding player-one -> only player-two receives!
      server.Broadcast(0, "announcement"u8, exclude: serverSideP1);

      string? c1Got = null;
      string? c2Got = null;
      for (var i = 0; i < 20; i++)
      {
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         c1.Tick(); c2.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));

         c1.PollEvents(evt => { if (evt.Type == DatagramEventType.MessageReceived) c1Got = Encoding.UTF8.GetString(evt.Payload.Span); });
         c2.PollEvents(evt => { if (evt.Type == DatagramEventType.MessageReceived) c2Got = Encoding.UTF8.GetString(evt.Payload.Span); });
      }

      await Assert.That(c1Got).IsNull();
      await Assert.That(c2Got).IsEqualTo("announcement");

      await c1.DisposeAsync();
      await c2.DisposeAsync();
      await cBanned.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task PskMismatch_FailsAeadHandshakeAndDropsConnection()
   {
      var hub = new SimulatedNetworkHub(seed: 2);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9200);

      var serverOpts = CreateSecureOptions("server-secret"u8.ToArray());
      var clientOpts = CreateSecureOptions("wrong-secret"u8.ToArray());
      clientOpts.ConnectTimeout = TimeSpan.FromMilliseconds(600);

      var server = new DatagramHost(serverEp, serverOpts, hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), clientOpts, hub.CreateTransport(), hub.Time);
      server.Start();
      client.Start();

      var connectTask = client.ConnectAsync(serverEp);
      for (var i = 0; i < 60 && !connectTask.IsCompleted; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }

      var timedOut = false;
      try
      {
         await connectTask;
      }
      catch (TimeoutException)
      {
         timedOut = true;
      }

      await Assert.That(timedOut).IsTrue();
      await Assert.That(server.PeerCount).IsEqualTo(0);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task PeerMigratesEndPointSeamlessly_AndDisconnectsCleanlyOrOnTimeout()
   {
      var hub = new SimulatedNetworkHub(seed: 3);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9300);

      var server = new DatagramHost(serverEp, CreateSecureOptions(), hub.CreateTransport(), hub.Time);
      var clientTransport = hub.CreateTransport();
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 50001), CreateSecureOptions(), clientTransport, hub.Time);
      server.Start();
      client.Start();

      var connectTask = client.ConnectAsync(serverEp);
      for (var i = 0; i < 30 && !connectTask.IsCompleted; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
      }

      var clientPeer = await connectTask;
      server.PollEvents(_ => { });
      client.PollEvents(_ => { });

      // Simulate Wi-Fi/Cellular NAT port rebind!
      var migratedEp = hub.MigrateEndpoint(clientTransport, new IPEndPoint(IPAddress.Parse("10.0.0.77"), 55555));
      clientPeer.TrySend(0, "after-nat-rebind"u8);

      IPEndPoint? migratedTo = null;
      string? receivedMsg = null;

      for (var i = 0; i < 20; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));

         server.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.PeerMigrated) migratedTo = evt.NewEndPoint;
            if (evt.Type == DatagramEventType.MessageReceived) receivedMsg = Encoding.UTF8.GetString(evt.Payload.Span);
         });
      }

      await Assert.That(migratedTo).IsEqualTo(migratedEp);
      await Assert.That(receivedMsg).IsEqualTo("after-nat-rebind");

      // Now test graceful disconnect with reason payload
      clientPeer.Disconnect("quit"u8);
      DatagramDisconnectReason? disconnectReason = null;
      string? disconnectText = null;

      for (var i = 0; i < 15; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));

         server.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.PeerDisconnected)
            {
               disconnectReason = evt.DisconnectReason;
               disconnectText = Encoding.UTF8.GetString(evt.Payload.Span);
            }
         });
      }

      await Assert.That(disconnectReason).IsEqualTo(DatagramDisconnectReason.GracefulRemote);
      await Assert.That(disconnectText).IsEqualTo("quit");
      await Assert.That(server.PeerCount).IsEqualTo(0);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task SilentPeer_TimesOutAndCleansUpAfterPeerTimeout()
   {
      var hub = new SimulatedNetworkHub(seed: 55);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9350);

      var server = new DatagramHost(serverEp, CreateSecureOptions(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateSecureOptions(), hub.CreateTransport(), hub.Time);
      server.Start();
      client.Start();

      var connectTask = client.ConnectAsync(serverEp);
      for (var i = 0; i < 30 && !connectTask.IsCompleted; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(15));
      }

      await connectTask;
      server.PollEvents(_ => { });
      client.PollEvents(_ => { });
      await Assert.That(server.PeerCount).IsEqualTo(1);

      // Client goes completely dark (never ticks again); advance past PeerTimeout (3s)
      hub.AdvanceAndDeliver(TimeSpan.FromSeconds(3.5));
      server.Tick();

      DatagramDisconnectReason? reason = null;
      server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.PeerDisconnected) reason = evt.DisconnectReason;
      });

      await Assert.That(reason).IsEqualTo(DatagramDisconnectReason.Timeout);
      await Assert.That(server.PeerCount).IsEqualTo(0);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}
