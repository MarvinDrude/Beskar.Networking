using System.Buffers;
using System.Text;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Server;
using Beskar.Networking.Transports.Memory;

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
}
