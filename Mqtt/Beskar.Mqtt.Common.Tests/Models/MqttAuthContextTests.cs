using Beskar.Mqtt.Common.Generators;
using Beskar.Mqtt.Common.Models;
using Beskar.Mqtt.Protocol.Results;
using Beskar.Mqtt.Protocol.Packets;
using System.Buffers;

namespace Beskar.Mqtt.Common.Tests.Models;

public class MqttAuthContextTests
{
   [Test]
   public async Task AwaitNextAuthPacketAsync_ShouldReturnAuthPacketResult_WhenAuthDispatched()
   {
      // Arrange
      using var broker = new SignalBroker();
      var connAckTcs = new TaskCompletionSource<ClientConnectResult>();
      var receiveTcs = new TaskCompletionSource();
      var authTcs = new TaskCompletionSource<AuthPacketResult>();

      var context = new MqttAuthContext()
      {
         AuthPacket = AuthPacketResult.Create(new AuthPacket()),
         PacketSender = null!, // not needed for this test
         Broker = broker,
         ConnAckTask = connAckTcs.Task,
         ReceiveTask = receiveTcs.Task,
         AuthTask = authTcs.Task
      };

      // Act
      var waitTask = context.AwaitNextAuthPacketAsync();
      
      var expectedAuth = AuthPacketResult.Create(new AuthPacket());
      broker.TryDispatch(expectedAuth, 0);

      var result = await waitTask;

      // Assert
      await Assert.That(result).IsNotNull();
      await Assert.That(ReferenceEquals(result, expectedAuth)).IsTrue();
   }

   [Test]
   public async Task AwaitNextAuthPacketAsync_ShouldReturnNull_WhenConnAckDispatched()
   {
      // Arrange
      using var broker = new SignalBroker();
      var connAckTcs = new TaskCompletionSource<ClientConnectResult>();
      var receiveTcs = new TaskCompletionSource();
      var authTcs = new TaskCompletionSource<AuthPacketResult>();

      var context = new MqttAuthContext()
      {
         AuthPacket = AuthPacketResult.Create(new AuthPacket()),
         PacketSender = null!, // not needed for this test
         Broker = broker,
         ConnAckTask = connAckTcs.Task,
         ReceiveTask = receiveTcs.Task,
         AuthTask = authTcs.Task
      };

      // Act
      var waitTask = context.AwaitNextAuthPacketAsync();

      // Simulate ConnAck completion
      connAckTcs.SetResult(ClientConnectResult.Create(new ConnAckPacket()));

      var result = await waitTask;

      // Assert
      await Assert.That(result).IsNull();
   }

   [Test]
   public async Task AwaitNextAuthPacketAsync_ShouldReturnNull_WhenReceiveTaskCompletes()
   {
      // Arrange
      using var broker = new SignalBroker();
      var connAckTcs = new TaskCompletionSource<ClientConnectResult>();
      var receiveTcs = new TaskCompletionSource();
      var authTcs = new TaskCompletionSource<AuthPacketResult>();

      var context = new MqttAuthContext()
      {
         AuthPacket = AuthPacketResult.Create(new AuthPacket()),
         PacketSender = null!, // not needed for this test
         Broker = broker,
         ConnAckTask = connAckTcs.Task,
         ReceiveTask = receiveTcs.Task,
         AuthTask = authTcs.Task
      };

      // Act
      var waitTask = context.AwaitNextAuthPacketAsync();

      // Simulate ReceiveTask completion
      receiveTcs.SetResult();

      var result = await waitTask;

      // Assert
      await Assert.That(result).IsNull();
   }

   [Test]
   public async Task AwaitNextAuthPacketAsync_ShouldReturnAuthPacketResultImmediately_WhenAuthTaskAlreadyCompleted()
   {
      // Arrange
      using var broker = new SignalBroker();
      var connAckTcs = new TaskCompletionSource<ClientConnectResult>();
      var receiveTcs = new TaskCompletionSource();
      var authTcs = new TaskCompletionSource<AuthPacketResult>();

      var expectedAuth = AuthPacketResult.Create(new AuthPacket());
      authTcs.SetResult(expectedAuth);

      var context = new MqttAuthContext()
      {
         AuthPacket = AuthPacketResult.Create(new AuthPacket()),
         PacketSender = null!, // not needed for this test
         Broker = broker,
         ConnAckTask = connAckTcs.Task,
         ReceiveTask = receiveTcs.Task,
         AuthTask = authTcs.Task
      };

      // Act
      var result = await context.AwaitNextAuthPacketAsync();

      // Assert
      await Assert.That(result).IsNotNull();
      await Assert.That(ReferenceEquals(result, expectedAuth)).IsTrue();
   }
}
