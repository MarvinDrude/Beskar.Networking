using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttClientEventTests
{
   private static int _nextPort = 15000;
   private static int GetFreePort()
   {
      return Interlocked.Increment(ref _nextPort);
   }

   [Test]
   public async Task Client_ConnectionFlow_TriggersConnectingAndConnectedEvents()
   {
      var port = GetFreePort();

      // Start the server
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      // Create client
      var client = (MqttClient)MqttClientFactory.CreateTcp();

      var connectingTcs = new TaskCompletionSource<ClientConnectingContext>();
      client.Events.OnClientConnecting.Add((context, ct) =>
      {
         connectingTcs.TrySetResult(context);
         return ValueTask.CompletedTask;
      });

      var connectedTcs = new TaskCompletionSource<ClientConnectedContext>();
      client.Events.OnClientConnected.Add((context, ct) =>
      {
         connectedTcs.TrySetResult(context);
         return ValueTask.CompletedTask;
      });

      // Connect client
      var connectOptions = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, port))
         .WithCleanSession()
         .WithClientId("test-connection-events-client")
         .Build();

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();

      var connectingCtx = await connectingTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));
      var connectedCtx = await connectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(connectingCtx).IsNotNull();
      await Assert.That(connectingCtx.ConnectOptions.ClientIdUtf8Bytes.ToArray())
         .IsEquivalentTo(connectOptions.ClientIdUtf8Bytes.ToArray());

      await Assert.That(connectedCtx).IsNotNull();
      await Assert.That(connectedCtx.ConnectResult.ReasonCode).IsEqualTo(ConnectReasonCode.Success);

      // Clean up
      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task Client_PublishAndSubscribe_TriggersOnMessageReceive()
   {
      var port = GetFreePort();

      // Start the server
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      // Create client
      var client = (MqttClient)MqttClientFactory.CreateTcp();

      var messageReceivedTcs = new TaskCompletionSource<MessageReceiveContext>();
      client.Events.OnMessageReceive.Add((context, ct) =>
      {
         messageReceivedTcs.TrySetResult(context);
         return ValueTask.CompletedTask;
      });

      // Connect client
      var connectOptions = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, port))
         .WithCleanSession()
         .WithClientId("test-message-receive-client")
         .Build();

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();

      // Subscribe to topic
      var topic = "test/event/topic";
      var subscribeOptions = new SubscribeOptionsBuilder()
         .WithTopicFilter(topic, QualityOfServiceType.AtMostOnce)
         .Build();

      var subscribeResult = await client.SubscribeAsync(subscribeOptions);
      await Assert.That(subscribeResult.Failed).IsFalse();

      // Publish a message
      var payload = "Hello, MQTT Events!"u8.ToArray();
      var publishOptions = new PublishOptionsBuilder()
         .WithTopic(topic)
         .WithPayload(payload)
         .WithQualityOfService(QualityOfServiceType.AtMostOnce)
         .Build();

      var publishResult = await client.PublishAsync(publishOptions);
      await Assert.That(publishResult.Failed).IsFalse();

      // Await message receive event
      var receivedCtx = await messageReceivedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(receivedCtx).IsNotNull();
      await Assert.That(receivedCtx.Message.Topic).IsEqualTo(topic);
      await Assert.That(receivedCtx.Message.Payload.ToArray()).IsEquivalentTo(payload);

      // Clean up
      await client.DisconnectAsync(new DisconnectOptions());
   }

   [Test]
   public async Task Client_DisconnectAsync_TriggersOnClientDisconnected()
   {
      var port = GetFreePort();

      // Start the server
      await using var server = MqttServerFactory.CreateBuilder()
         .UseTcp(port)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();

      // Create client
      var client = (MqttClient)MqttClientFactory.CreateTcp();

      var disconnectedTcs = new TaskCompletionSource<ClientDisconnectedContext>();
      client.Events.OnClientDisconnected.Add((context, ct) =>
      {
         disconnectedTcs.TrySetResult(context);
         return ValueTask.CompletedTask;
      });

      // Connect client
      var connectOptions = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, port))
         .WithCleanSession()
         .WithClientId("test-disconnect-client")
         .Build();

      var connectResult = await client.ConnectAsync(connectOptions);
      await Assert.That(connectResult.Failed).IsFalse();

      // Graceful client disconnect
      await client.DisconnectAsync(new DisconnectOptions());

      var resultContext = await disconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(resultContext).IsNotNull();
      await Assert.That(resultContext.BeforeConnected).IsTrue();
      await Assert.That(client.IsConnected).IsFalse();
   }

   [Test]
   public async Task Client_ReceiveDisconnectPacket_TriggersOnClientDisconnected()
   {
      var mockClient = new MockNetworkClient();
      var client = new MqttClient(mockClient);

      // Force state to Connected (3)
      var stateField = typeof(MqttClient).GetField("_state", BindingFlags.NonPublic | BindingFlags.Instance);
      stateField?.SetValue(client, 3); // MqttClientConnectionState.Connected = 3

      var disconnectedTcs = new TaskCompletionSource<ClientDisconnectedContext>();
      client.Events.OnClientDisconnected.Add((context, ct) =>
      {
         disconnectedTcs.TrySetResult(context);
         return ValueTask.CompletedTask;
      });

      var disconnectPacket = new DisconnectPacket
      {
         ReasonCode = DisconnectReasonCode.MalformedPacket,
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      // Execute handlers to simulate incoming packet
      client.UpdateDisconnectPacket(disconnectPacket);
      await client.HandleDisconnect(disconnectPacket);

      var resultContext = await disconnectedTcs.Task.WaitAsync(TimeSpan.FromSeconds(5));

      await Assert.That(resultContext).IsNotNull();
      await Assert.That(resultContext.ReasonCode).IsEqualTo(DisconnectReasonCode.MalformedPacket);
      await Assert.That(resultContext.BeforeConnected).IsTrue();
      await Assert.That(client.IsConnected).IsFalse();
   }

   private class MockNetworkClient : INetworkClient
   {
      public TransportKind Transport => TransportKind.Unknown;
      public bool IsConnected => false;
      public NetworkClientStats Stats => default;
      public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(EndPoint endPoint,
         CancellationToken ct = default)
      {
         return ValueTask.FromResult<Result<INetworkSession, NetworkCodeError>>(
            new NetworkCodeError(1, "Mock connect is not supported"));
      }

      public ValueTask DisconnectAsync(CancellationToken ct = default)
      {
         return ValueTask.CompletedTask;
      }

      public ValueTask DisposeAsync()
      {
         return ValueTask.CompletedTask;
      }
   }
}
