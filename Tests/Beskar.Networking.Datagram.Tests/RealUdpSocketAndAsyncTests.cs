using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Networking.Datagram.Tests;

[NotInParallel]
public class RealUdpSocketAndAsyncTests
{
   [Test]
   public async Task RealLoopbackUdpSockets_AsyncCallbacksAndChannels_DeliverEncryptedMessages()
   {
      var serverOpts = new DatagramOptions
      {
         DispatchMode = DatagramEventDispatchMode.Async,
         ManualTick = false,
         TickInterval = TimeSpan.FromMilliseconds(10),
         TimeSyncInterval = TimeSpan.FromMilliseconds(50),
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Aead }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered)
      .AddChannel(1, DatagramDeliveryMode.UnreliableSequenced);

      var clientOpts = new DatagramOptions
      {
         DispatchMode = DatagramEventDispatchMode.Async,
         ManualTick = false,
         TickInterval = TimeSpan.FromMilliseconds(10),
         TimeSyncInterval = TimeSpan.FromMilliseconds(50),
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Aead }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered)
      .AddChannel(1, DatagramDeliveryMode.UnreliableSequenced);

      await using var server = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), serverOpts);
      var serverReceivedTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);

      server.Events.MessageReceived = (peer, channelId, mode, payload, ct) =>
      {
         if (channelId == 0)
         {
            var text = Encoding.UTF8.GetString(payload.Span);
            serverReceivedTcs.TrySetResult(text);
            peer.TrySend(0, Encoding.UTF8.GetBytes($"echo:{text}"));
         }
         return ValueTask.CompletedTask;
      };
      server.Start();

      await using var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), clientOpts);
      var clientEchoTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.MessageReceived = (peer, channelId, mode, payload, ct) =>
      {
         if (channelId == 0)
         {
            clientEchoTcs.TrySetResult(Encoding.UTF8.GetString(payload.Span));
         }
         return ValueTask.CompletedTask;
      };
      client.Start();

      var peer = await client.ConnectAsync(server.LocalEndPoint!, "live-player"u8.ToArray());
      await Assert.That(peer.State).IsEqualTo(DatagramPeerState.Connected);

      var bigPayload = new string('B', 3500);
      await Assert.That(peer.TrySend(0, Encoding.UTF8.GetBytes(bigPayload))).IsEqualTo(DatagramSendResult.Queued);

      var gotServer = await serverReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      var gotClient = await clientEchoTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(gotServer).IsEqualTo(bigPayload);
      await Assert.That(gotClient).IsEqualTo($"echo:{bigPayload}");
      await Assert.That(peer.Stats.DatagramsSent).IsGreaterThan(0);
      await Assert.That(peer.Stats.DatagramsReceived).IsGreaterThan(0);
   }

   [Test]
   public async Task ReadEventsAsync_StreamsConnectedAndMessageEventsCleanly()
   {
      var opts = new DatagramOptions
      {
         DispatchMode = DatagramEventDispatchMode.Async,
         ManualTick = false,
         TickInterval = TimeSpan.FromMilliseconds(10),
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Aead, AeadCipher = DatagramAeadCipher.ChaCha20Poly1305 }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered);

      await using var server = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), opts);
      server.Start();

      await using var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), opts);
      client.Start();

      var peer = await client.ConnectAsync(server.LocalEndPoint!);
      peer.TrySend(0, "streamed-via-channel"u8);

      using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(3));
      string? received = null;
      uint senderPeerId = 0;

      await foreach (var evt in server.ReadEventsAsync(cts.Token))
      {
         using (evt)
         {
            if (evt.Type == DatagramEventType.MessageReceived)
            {
               senderPeerId = evt.Peer.PeerId;
               received = Encoding.UTF8.GetString(evt.Payload.Span);
               break;
            }
         }
      }

      await Assert.That(senderPeerId).IsEqualTo(1u);
      await Assert.That(received).IsEqualTo("streamed-via-channel");
   }

   [Test]
   public async Task RealIpv6LoopbackSockets_AeadEncryptedAndFragmentedExchange_WorksEndToEnd()
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
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered)
      .AddChannel(1, DatagramDeliveryMode.UnreliableSequenced);

      await using var server = new DatagramHost(new IPEndPoint(IPAddress.IPv6Loopback, 0), MakeOpts());
      var serverGotTcs = new TaskCompletionSource<byte[]>(TaskCreationOptions.RunContinuationsAsynchronously);
      server.Events.MessageReceived = (peer, ch, _, payload, _) =>
      {
         if (ch == 0)
         {
            serverGotTcs.TrySetResult(payload.ToArray());
            peer.TrySend(0, "ipv6-ack"u8);
         }
         return ValueTask.CompletedTask;
      };
      server.Start();

      await using var client = new DatagramHost(new IPEndPoint(IPAddress.IPv6Loopback, 0), MakeOpts());
      var clientGotTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      client.Events.MessageReceived = (_, ch, _, payload, _) =>
      {
         if (ch == 0) clientGotTcs.TrySetResult(Encoding.UTF8.GetString(payload.Span));
         return ValueTask.CompletedTask;
      };
      client.Start();

      var peer = await client.ConnectAsync(server.LocalEndPoint!, "ipv6-client"u8.ToArray());
      await Assert.That(peer.RemoteEndPoint.AddressFamily).IsEqualTo(AddressFamily.InterNetworkV6);

      var bigV6Payload = new byte[5200];
      new Random(6).NextBytes(bigV6Payload);
      peer.TrySend(0, bigV6Payload);
      peer.TrySend(1, "v6-snapshot"u8);

      var gotServer = await serverGotTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      var gotClient = await clientGotTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(gotServer).IsEquivalentTo(bigV6Payload);
      await Assert.That(gotClient).IsEqualTo("ipv6-ack");
   }

   [Test]
   public async Task RealUdpSockets_FiveConcurrentClients_TrulyAsyncValidateConnection_AndBroadcastLifecycle()
   {
      DatagramOptions MakeOpts() => new DatagramOptions
      {
         DispatchMode = DatagramEventDispatchMode.Async,
         ManualTick = false,
         TickInterval = TimeSpan.FromMilliseconds(10),
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Aead }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered)
      .AddChannel(1, DatagramDeliveryMode.ReliableUnordered);

      await using var server = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts());
      var connectedNames = new ConcurrentBag<string>();
      var disconnectedCount = 0;
      var allDisconnectedTcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

      // Genuinely async ValidateConnection simulating an async auth/database lookup!
      server.Events.ValidateConnection = async ctx =>
      {
         await Task.Delay(15);
         ctx.PeerTag = Encoding.UTF8.GetString(ctx.ConnectMetadata.Span);
      };
      server.Events.PeerConnected = async (peer, _) =>
      {
         await Task.Yield();
         connectedNames.Add((string)peer.Tag!);
      };
      server.Events.PeerDisconnected = async (_, _, _, _) =>
      {
         await Task.Yield();
         if (Interlocked.Increment(ref disconnectedCount) == 5)
            allDisconnectedTcs.TrySetResult();
      };
      server.Start();

      var clients = new DatagramHost[5];
      var broadcastTcs = new TaskCompletionSource<string>[5];

      for (var i = 0; i < 5; i++)
      {
         var idx = i;
         broadcastTcs[idx] = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
         clients[idx] = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts());
         clients[idx].Events.MessageReceived = (_, _, _, payload, _) =>
         {
            broadcastTcs[idx].TrySetResult(Encoding.UTF8.GetString(payload.Span));
            return ValueTask.CompletedTask;
         };
         clients[idx].Start();
      }

      var connectTasks = Enumerable.Range(0, 5)
         .Select(i => clients[i].ConnectAsync(server.LocalEndPoint!, Encoding.UTF8.GetBytes($"player-{i}")))
         .ToArray();

      var clientPeers = await Task.WhenAll(connectTasks).WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(server.PeerCount).IsEqualTo(5);

      server.Broadcast(0, "match-starting!"u8);

      var receivedAll = await Task.WhenAll(broadcastTcs.Select(t => t.Task)).WaitAsync(TimeSpan.FromSeconds(5));
      foreach (var msg in receivedAll)
      {
         await Assert.That(msg).IsEqualTo("match-starting!");
      }

      foreach (var p in clientPeers) p.Disconnect("done"u8);
      await allDisconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      await Assert.That(connectedNames.Count).IsEqualTo(5);
      await Assert.That(server.PeerCount).IsEqualTo(0);

      foreach (var c in clients) await c.DisposeAsync();
   }

   [Test]
   public async Task RealUdpSockets_AbruptDisposeMidBurst_ShutsDownCleanlyWithoutDeadlock()
   {
      DatagramOptions MakeOpts() => new DatagramOptions
      {
         DispatchMode = DatagramEventDispatchMode.Async,
         ManualTick = false,
         TickInterval = TimeSpan.FromMilliseconds(5),
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Aead }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered)
      .AddChannel(1, DatagramDeliveryMode.UnreliableSequenced);

      var server = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts());
      server.Events.MessageReceived = (_, _, _, _, _) => ValueTask.CompletedTask;
      server.Start();

      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts());
      client.Start();

      var peer = await client.ConnectAsync(server.LocalEndPoint!);
      var chunk = new byte[4500];
      for (var i = 0; i < 40; i++)
      {
         peer.TrySend(0, chunk);
         peer.TrySend(1, BitConverter.GetBytes(i));
      }

      // Immediately dispose both hosts mid-burst!
      var disposeBoth = Task.WhenAll(client.DisposeAsync().AsTask(), server.DisposeAsync().AsTask());
      await disposeBoth.WaitAsync(TimeSpan.FromSeconds(3));
   }

   [Test]
   public async Task AsyncDispatchMode_FiresAllFourLifecycleCallbacksIncludingPeerMigrated()
   {
      var hub = new SimulatedNetworkHub(seed: 4001);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9988);
      var oldClientEp = new IPEndPoint(IPAddress.Loopback, 55101);
      var newClientEp = new IPEndPoint(IPAddress.Parse("10.20.30.40"), 55102);

      DatagramOptions MakeOpts(DatagramEventDispatchMode mode) => new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = mode,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero,
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Aead, AllowEndpointMigration = true }
      }.AddChannel(0, DatagramDeliveryMode.ReliableOrdered);

      var connectedTcs = new TaskCompletionSource<uint>(TaskCreationOptions.RunContinuationsAsynchronously);
      var migratedTcs = new TaskCompletionSource<(IPEndPoint OldEp, IPEndPoint NewEp)>(TaskCreationOptions.RunContinuationsAsynchronously);
      var messageTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
      var disconnectedTcs = new TaskCompletionSource<DatagramDisconnectReason>(TaskCreationOptions.RunContinuationsAsynchronously);

      var server = new DatagramHost(serverEp, MakeOpts(DatagramEventDispatchMode.Async), hub.CreateTransport(), hub.Time);
      server.Events.PeerConnected = async (p, _) => { await Task.Yield(); connectedTcs.TrySetResult(p.PeerId); };
      server.Events.PeerMigrated = async (_, oldEp, newEp, _) => { await Task.Yield(); migratedTcs.TrySetResult((oldEp, newEp)); };
      server.Events.MessageReceived = async (_, _, _, payload, _) => { await Task.Yield(); messageTcs.TrySetResult(Encoding.UTF8.GetString(payload.Span)); };
      server.Events.PeerDisconnected = async (_, reason, _, _) => { await Task.Yield(); disconnectedTcs.TrySetResult(reason); };
      server.Start();

      var ct = hub.CreateTransport();
      var client = new DatagramHost(oldClientEp, MakeOpts(DatagramEventDispatchMode.Poll), ct, hub.Time);
      client.Start();

      var t = client.ConnectAsync(serverEp);
      for (var i = 0; i < 20 && !t.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var peer = await t;

      var connectedId = await connectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(2));
      await Assert.That(connectedId).IsEqualTo(1u);

      hub.MigrateEndpoint(ct, newClientEp);
      peer.TrySend(0, "migrated-async-msg"u8);
      client.Tick();
      server.Tick();

      var mig = await migratedTcs.Task.WaitAsync(TimeSpan.FromSeconds(2));
      var msg = await messageTcs.Task.WaitAsync(TimeSpan.FromSeconds(2));
      await Assert.That(mig.OldEp).IsEqualTo(oldClientEp);
      await Assert.That(mig.NewEp).IsEqualTo(newClientEp);
      await Assert.That(msg).IsEqualTo("migrated-async-msg");

      peer.Disconnect("done"u8);
      client.Tick();
      server.Tick();

      var reason = await disconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(2));
      await Assert.That(reason).IsEqualTo(DatagramDisconnectReason.GracefulRemote);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}
