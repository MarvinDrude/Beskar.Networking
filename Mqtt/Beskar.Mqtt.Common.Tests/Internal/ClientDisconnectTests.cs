using System.Buffers;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using Beskar.Memory.Results;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class ClientDisconnectTests
{
   private static int GetFreePort()
   {
      using var socket = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
      socket.Bind(new IPEndPoint(IPAddress.Loopback, 0));
      return ((IPEndPoint)socket.LocalEndPoint!).Port;
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
      await Assert.That(client.IsConnected).IsTrue();

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
      public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(EndPoint endPoint, CancellationToken ct = default)
      {
         return ValueTask.FromResult<Result<INetworkSession, NetworkCodeError>>(new NetworkCodeError(0, "Mock connect is not supported"));
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
