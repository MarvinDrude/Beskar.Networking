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
   public async Task CollisionPingPong_ShouldCompleteInLifoOrder()
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
      var awaiter1 = broker.AddAwaitable<PingResponse>(0);
      var awaiter2 = broker.AddAwaitable<PubAckResponse>(10);

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

      // Disposing the awaiters should now safely return them to the pool
      awaiter1.Dispose();
      awaiter2.Dispose();

      using var newBroker = new SignalBroker();
      var reused1 = SignalAwaiterPool<PingResponse>.Get(0, newBroker);
      var reused2 = SignalAwaiterPool<PubAckResponse>.Get(10, newBroker);

      await Assert.That(ReferenceEquals(reused1, awaiter1)).IsTrue();
      await Assert.That(ReferenceEquals(reused2, awaiter2)).IsTrue();

      reused1.Dispose();
      reused2.Dispose();
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
   public async Task ThreadSafetyStressTest_ShouldNotHangOrCorrupt()
   {
      // Arrange
      var broker = new SignalBroker();
      var cts = new CancellationTokenSource();

      cts.CancelAfter(TimeSpan.FromSeconds(8));

      const int taskCount = 10;
      const int iterationsPerTask = 2000;

      var tasks = new List<Task>();

      for (var t = 0; t < taskCount; t++)
      {
         tasks.Add(Task.Run(async () =>
         {
            var random = new Random();
            for (var i = 0; i < iterationsPerTask; i++)
            {
               if (cts.Token.IsCancellationRequested)
               {
                  break;
               }

               var id = (ushort)random.Next(0, 5); // Low range to force collisions
               var action = random.Next(0, 3);

               if (action == 0)
               {
                  using var awaiter = broker.AddAwaitable<PingResponse>(id);
                  using var waitCts = CancellationTokenSource.CreateLinkedTokenSource(cts.Token);

                  waitCts.CancelAfter(random.Next(1, 5));
                  try
                  {
                     await awaiter.WaitOneAsync(waitCts.Token);
                  }
                  catch (Exception)
                  {
                     // Expected timeout/cancellation
                  }
               }
               else if (action == 1)
               {
                  var awaiter = broker.AddAwaitable<PubAckResponse>(id);
                  _ = Task.Run(() =>
                  {
                     broker.TryDispatch(new PubAckResponse(), id);
                  }, cts.Token);

                  try
                  {
                     await awaiter.WaitOneAsync(cts.Token);
                  }
                  catch (Exception)
                  {
                     // Expected cancellation
                  }
                  finally
                  {
                     awaiter.Dispose();
                  }
               }
               else
               {
                  // Just dispatch
                  broker.TryDispatch(new PingResponse(), id);
                  broker.TryDispatch(new PubAckResponse(), id);
               }
            }
         }, cts.Token));
      }

      // Act & Assert
      try
      {
         await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10), CancellationToken.None);
      }
      catch (TimeoutException)
      {
         Assert.Fail("The stress test timed out, indicating a deadlock, cycle, or infinite loop.");
      }
   }

   [Test]
   public async Task AddAwaitable_ConcurrentlyWithDispose_ShouldThrowObjectDisposedExceptionAndNotHang()
   {
      // Arrange
      var broker = new SignalBroker();
      var startBarrier = new Barrier(3);

      var t1 = Task.Run(() =>
      {
         startBarrier.SignalAndWait();
         try
         {
            while (true)
            {
               using var awaiter = broker.AddAwaitable<PingResponse>(0);
            }
         }
         catch (ObjectDisposedException)
         {
            // Expected
         }
      });

      var t2 = Task.Run(() =>
      {
         startBarrier.SignalAndWait();
         try
         {
            while (true)
            {
               using var awaiter = broker.AddAwaitable<PubAckResponse>(0);
            }
         }
         catch (ObjectDisposedException)
         {
            // Expected
         }
      });

      var t3 = Task.Run(async () =>
      {
         startBarrier.SignalAndWait();
         // Give t1 and t2 a tiny moment to start calling AddAwaitable
         await Task.Delay(1);
         broker.Dispose();
      });

      // Act & Assert
      try
      {
         await Task.WhenAll(t1, t2, t3).WaitAsync(TimeSpan.FromSeconds(5));
      }
      catch (TimeoutException)
      {
         Assert.Fail("AddAwaitable hung concurrently with Dispose.");
      }
   }

   [Test]
   public async Task CollisionDifferentTypes_CompletedNonHeadMultipleSameType_ShouldCompleteAll()
   {
      // Arrange
      using var broker = new SignalBroker();

      using var awaiterB = broker.AddAwaitable<PubAckResponse>(10);
      using var awaiterA1 = broker.AddAwaitable<PingResponse>(10);
      using var awaiterA2 = broker.AddAwaitable<PingResponse>(10);

      var taskB = awaiterB.WaitOneAsync(CancellationToken.None).AsTask();
      var taskA1 = awaiterA1.WaitOneAsync(CancellationToken.None).AsTask();
      var taskA2 = awaiterA2.WaitOneAsync(CancellationToken.None).AsTask();

      var dispatched1 = broker.TryDispatch(new PingResponse(), 10);
      await Assert.That(dispatched1).IsTrue();
      await taskA2;
      await Assert.That(taskA1.Status == TaskStatus.RanToCompletion).IsFalse();

      var dispatched2 = broker.TryDispatch(new PingResponse(), 10);
      await Assert.That(dispatched2).IsTrue();
      await taskA1;
   }

   [Test]
   public async Task Reset_ShouldCancelPendingAwaitersAndAllowReactivation()
   {
      // Arrange
      using var broker = new SignalBroker();

      using var awaiter1 = broker.AddAwaitable<PingResponse>(0);
      using var awaiter2 = broker.AddAwaitable<PubAckResponse>(10);

      var task1 = awaiter1.WaitOneAsync(CancellationToken.None).AsTask();
      var task2 = awaiter2.WaitOneAsync(CancellationToken.None).AsTask();

      // Act - Reset the broker
      broker.Reset();

      // Assert - Pending tasks should be canceled
      var exception1Thrown = false;
      try
      {
         await task1;
      }
      catch (OperationCanceledException ex) when (ex.Message.Contains("The broker was reset."))
      {
         exception1Thrown = true;
      }
      await Assert.That(exception1Thrown).IsTrue();

      var exception2Thrown = false;
      try
      {
         await task2;
      }
      catch (OperationCanceledException ex) when (ex.Message.Contains("The broker was reset."))
      {
         exception2Thrown = true;
      }
      await Assert.That(exception2Thrown).IsTrue();

      // Assert - Awaiters can be disposed safely to return to pool
      awaiter1.Dispose();
      awaiter2.Dispose();

      // Act - Register new awaitables on the reset broker
      using var awaiter3 = broker.AddAwaitable<PingResponse>(0);
      var task3 = awaiter3.WaitOneAsync(CancellationToken.None).AsTask();

      var dispatched = broker.TryDispatch(new PingResponse(), 0);
      await Assert.That(dispatched).IsTrue();

      var result = await task3;
      await Assert.That(result).IsNotNull();
   }
}
