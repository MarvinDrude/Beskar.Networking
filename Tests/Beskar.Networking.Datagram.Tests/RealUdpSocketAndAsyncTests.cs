using System.Net;
using System.Text;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;

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
}
