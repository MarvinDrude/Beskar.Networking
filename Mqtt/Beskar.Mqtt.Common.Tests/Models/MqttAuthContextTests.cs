using Beskar.Mqtt.Common.Generators;
using Beskar.Mqtt.Common.Models;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Results;

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

      var context = new MqttAuthContext
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
      authTcs.SetResult(expectedAuth);

      var result = await waitTask;

      // Assert
      await Assert.That(result).IsNotNull();
      await Assert.That(ReferenceEquals(result, expectedAuth)).IsTrue();
   }

   [Test]
   public async Task AwaitNextAuthPacketAsync_ShouldAwaitBrokerDispatchedPacket_OnSubsequentCalls()
   {
      // Arrange
      using var broker = new SignalBroker();
      var connAckTcs = new TaskCompletionSource<ClientConnectResult>();
      var receiveTcs = new TaskCompletionSource();
      var authTcs = new TaskCompletionSource<AuthPacketResult>();

      var context = new MqttAuthContext
      {
         AuthPacket = AuthPacketResult.Create(new AuthPacket()),
         PacketSender = null!, // not needed for this test
         Broker = broker,
         ConnAckTask = connAckTcs.Task,
         ReceiveTask = receiveTcs.Task,
         AuthTask = authTcs.Task
      };

      // Complete the first call via authTcs
      var firstExpected = AuthPacketResult.Create(new AuthPacket());
      authTcs.SetResult(firstExpected);
      var firstResult = await context.AwaitNextAuthPacketAsync();
      await Assert.That(ReferenceEquals(firstResult, firstExpected)).IsTrue();

      // Complete the second call via broker.TryDispatch
      var secondExpected = AuthPacketResult.Create(new AuthPacket());
      var waitTask = context.AwaitNextAuthPacketAsync();

      broker.TryDispatch(secondExpected, 0);

      var secondResult = await waitTask;

      // Assert
      await Assert.That(secondResult).IsNotNull();
      await Assert.That(ReferenceEquals(secondResult, secondExpected)).IsTrue();
   }

   [Test]
   public async Task AwaitNextAuthPacketAsync_ShouldReturnNull_WhenConnAckDispatched()
   {
      // Arrange
      using var broker = new SignalBroker();
      var connAckTcs = new TaskCompletionSource<ClientConnectResult>();
      var receiveTcs = new TaskCompletionSource();
      var authTcs = new TaskCompletionSource<AuthPacketResult>();

      var context = new MqttAuthContext
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

      var context = new MqttAuthContext
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

      var context = new MqttAuthContext
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
