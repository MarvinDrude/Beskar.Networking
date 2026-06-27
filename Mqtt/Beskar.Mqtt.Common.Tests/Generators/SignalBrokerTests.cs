using System;
using System.Threading;
using System.Threading.Tasks;
using Beskar.Mqtt.Common.Generators;

namespace Beskar.Mqtt.Common.Tests.Generators;

public class SignalBrokerTests
{
   private class PingResponse { }
   private class PubAckResponse { }
   private class SubAckResponse { }

   [Test]
   public async Task SingleRequestResponse_ShouldCompleteSuccessfully()
   {
      // Arrange
      using var broker = new SignalBroker();
      using var awaiter = broker.AddAwaitable<PubAckResponse>(100);

      // Act
      var waitTask = awaiter.WaitOneAsync(CancellationToken.None).AsTask();
      var dispatched = broker.TryDispatch(new PubAckResponse(), 100);

      // Assert
      await Assert.That(dispatched).IsTrue();
      var result = await waitTask;
      await Assert.That(result).IsNotNull();
   }

   [Test]
   public async Task MultipleConcurrentUniques_ShouldDispatchCorrectly()
   {
      // Arrange
      using var broker = new SignalBroker();
      var tasks = new Task<PubAckResponse>[10];
      var awaiters = new SignalAwaiter<PubAckResponse>[10];

      for (ushort i = 0; i < 10; i++)
      {
         var id = (ushort)(i + 1);
         var awaiter = broker.AddAwaitable<PubAckResponse>(id);

         awaiters[i] = awaiter;
         tasks[i] = awaiter.WaitOneAsync(CancellationToken.None).AsTask();
      }

      // Act & Assert
      for (ushort i = 0; i < 10; i++)
      {
         var id = (ushort)(i + 1);
         var dispatched = broker.TryDispatch(new PubAckResponse(), id);
         await Assert.That(dispatched).IsTrue();

         var result = await tasks[i];
         await Assert.That(result).IsNotNull();
         awaiters[i].Dispose();
      }
   }

   [Test]
   public async Task CollisionPingPong_ShouldCompleteInFifoOrder()
   {
      // Arrange
      using var broker = new SignalBroker();

      // Add two awaitables on ID 0 (representing two concurrent pings)
      using var awaiter1 = broker.AddAwaitable<PingResponse>(0);
      using var awaiter2 = broker.AddAwaitable<PingResponse>(0);

      // Act
      var waitTask1 = awaiter1.WaitOneAsync(CancellationToken.None).AsTask();
      var waitTask2 = awaiter2.WaitOneAsync(CancellationToken.None).AsTask();

      // Dispatching first PingResponse (should match awaiter2 because it is the head)
      var dispatched1 = broker.TryDispatch(new PingResponse(), 0);
      await Assert.That(dispatched1).IsTrue();

      // Dispatching second PingResponse (should match awaiter1)
      var dispatched2 = broker.TryDispatch(new PingResponse(), 0);
      await Assert.That(dispatched2).IsTrue();

      // Assert
      var res2 = await waitTask2;
      var res1 = await waitTask1;

      await Assert.That(res1).IsNotNull();
      await Assert.That(res2).IsNotNull();
   }

   [Test]
   public async Task TimeoutAndCancellationPruning_ShouldPruneAndPoolCorrectly()
   {
      // Arrange
      using var broker = new SignalBroker();
      using var cts = new CancellationTokenSource();

      // Add two awaitables on ID 0 (head is awaiter2)
      var awaiter1 = broker.AddAwaitable<PingResponse>(0);
      var awaiter2 = broker.AddAwaitable<PingResponse>(0);

      // Start awaiting
      var task1 = awaiter1.WaitOneAsync(cts.Token).AsTask();
      var task2 = awaiter2.WaitOneAsync(CancellationToken.None).AsTask();

      // Cancel the first awaiter (which is in the middle of the chain)
      await cts.CancelAsync();

      // Assert that awaiting it throws TimeoutException (or OperationCanceledException)
      var exceptionThrown = false;
      try
      {
         await task1;
      }
      catch (Exception)
      {
         exceptionThrown = true;
      }
      await Assert.That(exceptionThrown).IsTrue();

      // Dispose awaiter1 (should not pool yet because it's not the head of the chain)
      awaiter1.Dispose();

      // Now complete the head awaiter2
      var dispatched = broker.TryDispatch(new PingResponse(), 0);
      await Assert.That(dispatched).IsTrue();
      await task2;
      awaiter2.Dispose();

      // Now add a new awaiter on ID 0. This should prune the dead awaiter1!
      var awaiter3 = broker.AddAwaitable<PingResponse>(0);

      // Verify that awaiter1 was pruned and thus returned to the pool.
      // We can verify this by checking if the next awaiter we get from the pool is awaiter1 reference!
      var reusedAwaiter = SignalAwaiterPool<PingResponse>.Get(0, broker);
      await Assert.That(ReferenceEquals(reusedAwaiter, awaiter1)).IsTrue();

      // Clean up
      awaiter3.Dispose();
      reusedAwaiter.Dispose();
   }

   [Test]
   public async Task BrokerDisposal_ShouldFailAllPendingAwaitables()
   {
      // Arrange
      var broker = new SignalBroker();
      using var awaiter1 = broker.AddAwaitable<PingResponse>(0);
      using var awaiter2 = broker.AddAwaitable<PubAckResponse>(10);

      var task1 = awaiter1.WaitOneAsync(CancellationToken.None).AsTask();
      var task2 = awaiter2.WaitOneAsync(CancellationToken.None).AsTask();

      // Act
      broker.Dispose();

      // Assert
      var exception1Thrown = false;
      try
      {
         await task1;
      }
      catch (ObjectDisposedException)
      {
         exception1Thrown = true;
      }
      await Assert.That(exception1Thrown).IsTrue();

      var exception2Thrown = false;
      try
      {
         await task2;
      }
      catch (ObjectDisposedException)
      {
         exception2Thrown = true;
      }
      await Assert.That(exception2Thrown).IsTrue();
   }

   [Test]
   public async Task TryDispatchPruning_ShouldPruneDeadHeadAndCompleteNext()
   {
      // Arrange
      using var broker = new SignalBroker();
      using var cts = new CancellationTokenSource();

      var awaiter1 = broker.AddAwaitable<PingResponse>(0);
      var awaiter2 = broker.AddAwaitable<PingResponse>(0); // Head

      var task1 = awaiter1.WaitOneAsync(CancellationToken.None).AsTask();
      var task2 = awaiter2.WaitOneAsync(cts.Token).AsTask();

      // Cancel head
      await cts.CancelAsync();
      try
      {
         await task2;
      }
      catch
      {
         // ignore
      }
      awaiter2.Dispose();

      // Act
      // TryDispatch should detect awaiter2 is dead, prune it, and complete awaiter1
      var dispatched = broker.TryDispatch(new PingResponse(), 0);

      // Assert
      await Assert.That(dispatched).IsTrue();
      var res1 = await task1;
      await Assert.That(res1).IsNotNull();

      awaiter1.Dispose();
   }
}
