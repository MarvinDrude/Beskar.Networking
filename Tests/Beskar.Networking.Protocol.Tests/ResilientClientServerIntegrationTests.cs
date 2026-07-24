using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Text;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Server;
using Beskar.Networking.Transports.Memory;
using Beskar.Networking.Transports.Quic;

namespace Beskar.Networking.Protocol.Tests;

public class ResilientClientServerIntegrationTests
{
   [Test]
   public async Task ResilientClient_And_ResilientServer_ShouldConnect_And_PerformHandshake()
   {
      var endpoint = new MemoryEndPoint("resilient_test_channel_1");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var serverOptions = new ResilientServerOptions();
      var server = new ResilientServer<BeskarPacket>([listener], serverOptions);

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>();
      var connectResult = await client.ConnectAsync(endpoint);

      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();
      await Assert.That(server.Clients.Count).IsEqualTo(1);

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ResilientClient_And_ResilientServer_ShouldExchangeMessages()
   {
      var endpoint = new MemoryEndPoint("resilient_test_channel_2");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());

      var receivedTcs = new TaskCompletionSource<string>();

      server.Events.FrameReceived.Add((ctx, _) =>
      {
         var text = Encoding.UTF8.GetString(ctx.Frame.Payload.ToArray());
         receivedTcs.TrySetResult(text);
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>();
      await client.ConnectAsync(endpoint);

      var payload = "Hello Resilient Networking!"u8.ToArray();
      var frame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(payload));
      await client.SendAsync(frame);

      var receivedText = await receivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
      await Assert.That(receivedText).IsEqualTo("Hello Resilient Networking!");

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ResilientClient_DisconnectWithPayload_ShouldDeliverPayloadToServer()
   {
      var endpoint = new MemoryEndPoint("resilient_test_channel_3");
      var listener = new MemoryNetworkListener(endpoint, new MemoryTransportOptions());
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());

      var disconnectTcs = new TaskCompletionSource<DisconnectPacketPayload>();

      server.Events.ClientDisconnected.Add((ctx, _) =>
      {
         if (ctx.Client.DisconnectPayload != null) disconnectTcs.TrySetResult(ctx.Client.DisconnectPayload);
         return ValueTask.CompletedTask;
      });

      await server.StartAsync();

      var client = ResilientClientFactory.CreateMemory<BeskarPacket>();
      await client.ConnectAsync(endpoint);

      var disconnectPayload = new DisconnectPacketPayload
      {
         ReasonCode = 0x99,
         ReasonString = "Graceful client disconnect"
      };

      await client.DisconnectAsync(disconnectPayload);

      var receivedPayload = await disconnectTcs.Task.WaitAsync(TimeSpan.FromSeconds(3));
      await Assert.That(receivedPayload.ReasonCode).IsEqualTo((byte)0x99);
      await Assert.That(receivedPayload.ReasonString).IsEqualTo("Graceful client disconnect");

      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ResilientClient_And_ResilientServer_ShouldExchangeMessages_OverMultipleClientOpenedStreams()
   {
      if (!QuicConnection.IsSupported)
         return;

      var clientSslOptions = new SslClientAuthenticationOptions
      {
         ApplicationProtocols = [new SslApplicationProtocol("beskar-quic")],
         RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
      };

      var transportOptions = new QuicTransportOptions
      {
         SslClientOptions = clientSslOptions
      };

      var endpoint = new IPEndPoint(IPAddress.Loopback, 0);
      var listener = new QuicNetworkListener(endpoint, transportOptions);
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());

      var receivedMessages = new ConcurrentDictionary<long, string>();
      var expectedMessages = new Dictionary<int, string>();
      var messageCount = 5;
      var tcs = new TaskCompletionSource();
      var receivedCount = 0;

      server.Events.FrameReceived.Add((ctx, _) =>
      {
         var text = Encoding.UTF8.GetString(ctx.Frame.Payload.ToArray());
         receivedMessages[ctx.Stream.StreamId] = text;
         if (Interlocked.Increment(ref receivedCount) == messageCount)
         {
            tcs.TrySetResult();
         }
         return ValueTask.CompletedTask;
      });

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var client = ResilientClientFactory.CreateQuic<BeskarPacket>(transportOptions);
      var connectResult = await client.ConnectAsync(listener.LocalAddress);

      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      var streams = new List<INetworkStream>();
      for (var i = 0; i < messageCount; i++)
      {
         var openResult = await client.OpenStreamAsync();
         await Assert.That(openResult.Failed).IsFalse();
         var stream = openResult.Success!;
         streams.Add(stream);

         var msg = $"Msg from Client stream {i}";
         expectedMessages[i] = msg;

         var payload = Encoding.UTF8.GetBytes(msg);
         var frame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(payload));
         await client.SendAsync(frame, stream);
      }

      await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(receivedMessages.Count).IsEqualTo(messageCount);
      for (var i = 0; i < messageCount; i++)
      {
         var stream = streams[i];
         await Assert.That(receivedMessages.TryGetValue(stream.StreamId, out var receivedText)).IsTrue();
         await Assert.That(receivedText).IsEqualTo(expectedMessages[i]);
      }

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ResilientClient_And_ResilientServer_ShouldExchangeMessages_OverMultipleServerOpenedStreams()
   {
      if (!QuicConnection.IsSupported)
         return;

      var clientSslOptions = new SslClientAuthenticationOptions
      {
         ApplicationProtocols = [new SslApplicationProtocol("beskar-quic")],
         RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
      };

      var transportOptions = new QuicTransportOptions
      {
         SslClientOptions = clientSslOptions
      };

      var endpoint = new IPEndPoint(IPAddress.Loopback, 0);
      var listener = new QuicNetworkListener(endpoint, transportOptions);
      var server = new ResilientServer<BeskarPacket>([listener], new ResilientServerOptions());

      var receivedMessages = new ConcurrentDictionary<long, string>();
      var expectedMessages = new Dictionary<int, string>();
      var messageCount = 5;
      var tcs = new TaskCompletionSource();
      var receivedCount = 0;

      var client = ResilientClientFactory.CreateQuic<BeskarPacket>(transportOptions);

      client.Events.FrameReceived.Add((ctx, _) =>
      {
         var text = Encoding.UTF8.GetString(ctx.Frame.Payload.ToArray());
         receivedMessages[ctx.Stream.StreamId] = text;
         if (Interlocked.Increment(ref receivedCount) == messageCount)
         {
            tcs.TrySetResult();
         }
         return ValueTask.CompletedTask;
      });

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      var connectResult = await client.ConnectAsync(listener.LocalAddress);
      await Assert.That(connectResult.Failed).IsFalse();
      await Assert.That(client.IsConnected).IsTrue();

      // Wait a moment for server to register the client connection
      for (var i = 0; i < 50 && server.Clients.Count == 0; i++)
      {
         await Task.Delay(50);
      }
      await Assert.That(server.Clients.Count).IsEqualTo(1);
      var serverClient = server.Clients.GetAll().First();

      var streams = new List<INetworkStream>();
      for (var i = 0; i < messageCount; i++)
      {
         var openResult = await serverClient.OpenStreamAsync();
         await Assert.That(openResult.Failed).IsFalse();
         var stream = openResult.Success!;
         streams.Add(stream);

         var msg = $"Msg from Server stream {i}";
         expectedMessages[i] = msg;

         var payload = Encoding.UTF8.GetBytes(msg);
         var frame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(payload));
         await serverClient.SendAsync(frame, stream);
      }

      await tcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(receivedMessages.Count).IsEqualTo(messageCount);
      for (var i = 0; i < messageCount; i++)
      {
         var stream = streams[i];
         await Assert.That(receivedMessages.TryGetValue(stream.StreamId, out var receivedText)).IsTrue();
         await Assert.That(receivedText).IsEqualTo(expectedMessages[i]);
      }

      await client.DisconnectAsync();
      await server.StopAsync();
      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}
