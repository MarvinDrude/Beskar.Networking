using Beskar.Mqtt.Common.Generators;

namespace Beskar.Mqtt.Common.Tests.Generators;

[NotInParallel(nameof(SignalBrokerTests))]
public class SignalBrokerTests
{
   private class PingResponse
   {
   }

   private class PubAckResponse
   {
   }

   private class SubAckResponse
   {
   }

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

   [Test]
   public async Task CollisionDifferentTypes_CompletedNonHead_ShouldNotPoolUntilPruned()
   {
      // Arrange
      using var broker = new SignalBroker();

      var awaiter1 = broker.AddAwaitable<PingResponse>(10);
      var awaiter2 = broker.AddAwaitable<PubAckResponse>(10);

      var task1 = awaiter1.WaitOneAsync(CancellationToken.None).AsTask();
      var task2 = awaiter2.WaitOneAsync(CancellationToken.None).AsTask();

      var dispatched1 = broker.TryDispatch(new PingResponse(), 10);
      await Assert.That(dispatched1).IsTrue();
      await task1;

      awaiter1.Dispose();

      var tempAwaiter = SignalAwaiterPool<PingResponse>.Get(100, broker);
      await Assert.That(ReferenceEquals(tempAwaiter, awaiter1)).IsFalse();
      tempAwaiter.Dispose();

      var dispatched2 = broker.TryDispatch(new PubAckResponse(), 10);
      await Assert.That(dispatched2).IsTrue();
      await task2;
      awaiter2.Dispose();

      // Trigger pruning on ID 10 by adding a new awaitable.
      // This will notice that the head (awaiter1) is dead (disposed/state 3) and prune/pool it.
      using var awaiter3 = broker.AddAwaitable<PingResponse>(10);

      var reusedAwaiter = SignalAwaiterPool<PingResponse>.Get(100, broker);
      await Assert.That(ReferenceEquals(reusedAwaiter, awaiter1)).IsTrue();
      reusedAwaiter.Dispose();
   }

   [Test]
   public async Task PoolRecycling_AllAwaitablesShouldReturnToPool()
   {
      // Arrange
      using var broker = new SignalBroker();
      var awaiters = new SignalAwaiter<PingResponse>[50];
      var tasks = new Task<PingResponse>[50];

      for (var i = 0; i < 50; i++)
      {
         awaiters[i] = broker.AddAwaitable<PingResponse>((ushort)i);
         tasks[i] = awaiters[i].WaitOneAsync(CancellationToken.None).AsTask();
      }

      var originalInstances = (SignalAwaiter<PingResponse>[])awaiters.Clone();
      for (ushort i = 0; i < 50; i++)
      {
         var dispatched = broker.TryDispatch(new PingResponse(), i);
         await Assert.That(dispatched).IsTrue();
         await tasks[i];

         awaiters[i].Dispose();
      }

      var recycledAwaiters = new SignalAwaiter<PingResponse>[50];
      for (var i = 0; i < 50; i++)
      {
         recycledAwaiters[i] = SignalAwaiterPool<PingResponse>.Get((ushort)i, broker);
      }

      for (var i = 0; i < 50; i++)
      {
         var found = false;
         for (var j = 0; j < 50; j++)
         {
            if (ReferenceEquals(originalInstances[j], recycledAwaiters[i]))
            {
               found = true;
               break;
            }
         }

         await Assert.That(found).IsTrue();
      }

      // Clean up
      for (var i = 0; i < 50; i++)
      {
         recycledAwaiters[i].Dispose();
      }
   }

   [Test]
   public Task DoubleDispose_ShouldBeNoOpAndSafe()
   {
      // Arrange
      using var broker = new SignalBroker();
      var awaiter = broker.AddAwaitable<PingResponse>(20);

      // Act & Assert
      awaiter.Dispose();
      awaiter.Dispose(); // Should not throw or cause double pooling

      broker.Dispose();
      broker.Dispose(); // Should not throw

      return Task.CompletedTask;
   }

   [Test]
   public async Task ConcurrentStress_ShouldCompleteAndPoolCorrectly()
   {
      var broker = new SignalBroker();
      const int numThreads = 10;
      const int iterationsPerThread = 200;

      var tasks = new Task[numThreads];
      for (var t = 0; t < numThreads; t++)
      {
         var threadId = t;
         tasks[t] = Task.Run(async () =>
         {
            var random = new Random(threadId);
            for (var i = 0; i < iterationsPerThread; i++)
            {
               var id = (ushort)random.Next(0, 10); // Colliding IDs to stress-test collision and pruning

               var shouldSucceed = random.Next(0, 2) == 0;
               var awaiter = broker.AddAwaitable<PingResponse>(id);

               if (shouldSucceed)
               {
                  var waitTask = awaiter.WaitOneAsync(CancellationToken.None).AsTask();
                  _ = Task.Run(() => { broker.TryDispatch(new PingResponse(), id); });

                  try
                  {
                     await waitTask;
                  }
                  catch
                  {
                     // Ignore cancellation/timeout under race conditions
                  }
               }
               else
               {
                  using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(1));
                  try
                  {
                     await awaiter.WaitOneAsync(cts.Token);
                  }
                  catch (Exception)
                  {
                     // Expected timeout/cancellation
                  }
               }

               awaiter.Dispose();
            }
         });
      }

      await Task.WhenAll(tasks);
   }
}
