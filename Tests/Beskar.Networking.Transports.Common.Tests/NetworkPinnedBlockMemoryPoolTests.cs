using System.Buffers;
using System.Diagnostics.Metrics;
using System.IO.Pipelines;
using System.Reflection;
using System.Runtime.InteropServices;
using Beskar.Networking.Abstractions.Telemetry;
using Beskar.Networking.Transports.Common.Pipelines;

namespace Beskar.Networking.Transports.Common.Tests;

[NotInParallel]
public class NetworkPinnedBlockMemoryPoolTests
{
   [Test]
   public async Task BlockSize_And_MaxBufferSize_ShouldBe4096()
   {
      using var pool = new NetworkPinnedBlockMemoryPool();

      await Assert.That(NetworkPinnedBlockMemoryPool.BlockSize).IsEqualTo(4096);
      await Assert.That(pool.MaxBufferSize).IsEqualTo(4096);
      await Assert.That(pool.MaxBufferSize).IsEqualTo(NetworkPinnedBlockMemoryPool.BlockSize);
   }

   [Test]
   public async Task InitialCounters_ShouldAllBeZero()
   {
      using var pool = new NetworkPinnedBlockMemoryPool();

      await Assert.That(pool.CreatedBlocksCount).IsEqualTo(0);
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo(0);
      await Assert.That(pool.RentedBlocksCount).IsEqualTo(0);
   }

   [Test]
   [Arguments(-1)]
   [Arguments(0)]
   [Arguments(1)]
   [Arguments(64)]
   [Arguments(1024)]
   [Arguments(2048)]
   [Arguments(4095)]
   [Arguments(4096)]
   public async Task Rent_WithValidSizes_ShouldAlwaysReturn4096ByteBlock(int requestedSize)
   {
      using var pool = new NetworkPinnedBlockMemoryPool();
      using var owner = pool.Rent(requestedSize);

      await Assert.That(owner).IsNotNull();
      await Assert.That(owner.Memory.Length).IsEqualTo(4096);
   }

   [Test]
   [Arguments(4097)]
   [Arguments(8192)]
   [Arguments(65536)]
   [Arguments(int.MaxValue)]
   public async Task Rent_WithSizeGreaterThanBlockSize_ShouldThrowArgumentOutOfRangeException(int invalidSize)
   {
      using var pool = new NetworkPinnedBlockMemoryPool();

      await Assert.That(() => pool.Rent(invalidSize)).Throws<ArgumentOutOfRangeException>();
      await Assert.That(pool.CreatedBlocksCount).IsEqualTo(0);
      await Assert.That(pool.RentedBlocksCount).IsEqualTo(0);
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo(0);
   }

   [Test]
   public async Task Rent_And_Dispose_ShouldTrackCreatedInStoreAndRentedCountsAccurately()
   {
      using var pool = new NetworkPinnedBlockMemoryPool();

      var owner1 = pool.Rent();
      await Assert.That(pool.CreatedBlocksCount).IsEqualTo(1);
      await Assert.That(pool.RentedBlocksCount).IsEqualTo(1);
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo(0);

      var owner2 = pool.Rent();
      var owner3 = pool.Rent();
      await Assert.That(pool.CreatedBlocksCount).IsEqualTo(3);
      await Assert.That(pool.RentedBlocksCount).IsEqualTo(3);
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo(0);

      owner1.Dispose();
      await Assert.That(pool.CreatedBlocksCount).IsEqualTo(3);
      await Assert.That(pool.RentedBlocksCount).IsEqualTo(2);
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo(1);

      owner2.Dispose();
      owner3.Dispose();
      await Assert.That(pool.CreatedBlocksCount).IsEqualTo(3);
      await Assert.That(pool.RentedBlocksCount).IsEqualTo(0);
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo(3);
   }

   [Test]
   public async Task Rent_WhenBlocksAvailableInStore_ShouldReuseExistingBlocksWithoutCreatingNewOnes()
   {
      using var pool = new NetworkPinnedBlockMemoryPool();

      var firstOwner = pool.Rent();
      firstOwner.Dispose();

      await Assert.That(pool.CreatedBlocksCount).IsEqualTo(1);
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo(1);

      var secondOwner = pool.Rent();
      try
      {
         await Assert.That(ReferenceEquals(firstOwner, secondOwner)).IsTrue();
         await Assert.That(pool.CreatedBlocksCount).IsEqualTo(1);
         await Assert.That(pool.InStoreBlocksCount).IsEqualTo(0);
         await Assert.That(pool.RentedBlocksCount).IsEqualTo(1);
      }
      finally
      {
         secondOwner.Dispose();
      }
   }

   [Test]
   public async Task RentedBlock_PoolProperty_ShouldReferenceOriginatingPool()
   {
      using var pool = new NetworkPinnedBlockMemoryPool();
      using var owner = pool.Rent();

      var poolProp = owner.GetType().GetProperty("Pool", BindingFlags.Public | BindingFlags.Instance);
      await Assert.That(poolProp).IsNotNull();

      var originatingPool = poolProp!.GetValue(owner);
      await Assert.That(ReferenceEquals(originatingPool, pool)).IsTrue();
   }

   [Test]
   public async Task RentedBlock_MemoryIsPinned_AddressRemainsStableAcrossCompactingGC()
   {
      using var pool = new NetworkPinnedBlockMemoryPool();
      using var owner = pool.Rent();

      var hasArray = MemoryMarshal.TryGetArray<byte>(owner.Memory, out var segment);
      await Assert.That(hasArray).IsTrue();
      await Assert.That(segment.Array).IsNotNull();
      await Assert.That(segment.Offset).IsEqualTo(0);
      await Assert.That(segment.Count).IsEqualTo(4096);

      var addressBeforeGc = Marshal.UnsafeAddrOfPinnedArrayElement(segment.Array!, 0);

      // Allocate churn and trigger compacting full GCs to verify the block does not relocate
      for (var i = 0; i < 5; i++)
      {
         _ = new byte[1024 * 64];
         GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
         GC.WaitForPendingFinalizers();
      }

      var addressAfterGc = Marshal.UnsafeAddrOfPinnedArrayElement(segment.Array!, 0);
      await Assert.That(addressAfterGc).IsEqualTo(addressBeforeGc);

      using var handle = owner.Memory.Pin();
      var addressWhilePinned = Marshal.UnsafeAddrOfPinnedArrayElement(segment.Array!, 0);
      await Assert.That(addressWhilePinned).IsEqualTo(addressBeforeGc);
   }

   [Test]
   public async Task RentedBlock_DataIntegrity_WriteAndReadFull4096Bytes()
   {
      using var pool = new NetworkPinnedBlockMemoryPool();
      using var owner = pool.Rent();

      var span = owner.Memory;
      for (var i = 0; i < span.Length; i++)
      {
         span.Span[i] = (byte)((i * 31 + 17) & 0xFF);
      }

      for (var i = 0; i < span.Length; i++)
      {
         await Assert.That(span.Span[i]).IsEqualTo((byte)((i * 31 + 17) & 0xFF));
      }
   }

   [Test]
   public async Task Dispose_Pool_ShouldDrainStoredBlocks_AndBeIdempotent()
   {
      var pool = new NetworkPinnedBlockMemoryPool();

      var b1 = pool.Rent();
      var b2 = pool.Rent();
      var b3 = pool.Rent();

      b1.Dispose();
      b2.Dispose();
      b3.Dispose();

      await Assert.That(pool.InStoreBlocksCount).IsEqualTo(3);

      pool.Dispose();
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo(0);

      // Second Dispose should be a safe no-op
      pool.Dispose();
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo(0);
   }

   [Test]
   public async Task Rent_AfterPoolDisposed_ShouldThrowObjectDisposedException()
   {
      var pool = new NetworkPinnedBlockMemoryPool();
      pool.Dispose();

      await Assert.That(() => pool.Rent()).Throws<ObjectDisposedException>();
   }

   [Test]
   public async Task DisposeBlock_AfterPoolDisposed_ShouldNotThrow_AndShouldNotEnqueueBlock()
   {
      var pool = new NetworkPinnedBlockMemoryPool();
      var owner = pool.Rent();

      await Assert.That(pool.CreatedBlocksCount).IsEqualTo(1);
      await Assert.That(pool.RentedBlocksCount).IsEqualTo(1);

      pool.Dispose();
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo(0);

      // Disposing an in-flight block after the pool itself was disposed must not throw or re-add to _blocks
      owner.Dispose();
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo(0);
   }

   [Test]
   public async Task PipeIntegration_MultiBlockReadWrite_ShouldLeaseAndReturnAllBlocksCleanly()
   {
      using var pool = new NetworkPinnedBlockMemoryPool();
      var pipeOptions = new PipeOptions(
         pool: pool,
         minimumSegmentSize: NetworkPinnedBlockMemoryPool.BlockSize,
         pauseWriterThreshold: 0,
         resumeWriterThreshold: 0,
         useSynchronizationContext: false);

      var pipe = new Pipe(pipeOptions);

      // Write 32 KiB (8 full 4096-byte blocks)
      const int payloadSize = 32 * 1024;
      var payload = new byte[payloadSize];
      Random.Shared.NextBytes(payload);

      await pipe.Writer.WriteAsync(payload);
      await pipe.Writer.FlushAsync();

      await Assert.That(pool.CreatedBlocksCount).IsGreaterThanOrEqualTo(8);
      await Assert.That(pool.RentedBlocksCount).IsGreaterThanOrEqualTo(8);

      var readResult = await pipe.Reader.ReadAsync();
      await Assert.That(readResult.Buffer.Length).IsEqualTo(payloadSize);

      var actual = readResult.Buffer.ToArray();
      await Assert.That(actual.AsSpan().SequenceEqual(payload)).IsTrue();

      pipe.Reader.AdvanceTo(readResult.Buffer.End);
      await pipe.Writer.CompleteAsync();
      await pipe.Reader.CompleteAsync();

      // After completing the pipe, all rented blocks must be returned to the store
      await Assert.That(pool.RentedBlocksCount).IsEqualTo(0);
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo((int)pool.CreatedBlocksCount);
   }

   [Test]
   public async Task ConcurrentRentWriteVerifyAndReturn_UnderHighContention_ShouldMaintainInvariantsAndDataIsolation()
   {
      using var pool = new NetworkPinnedBlockMemoryPool();

      const int workerCount = 16;
      const int iterationsPerWorker = 250;

      var tasks = new Task<bool>[workerCount];
      for (var w = 0; w < workerCount; w++)
      {
         var workerId = (byte)(w + 1);
         tasks[w] = Task.Run((async Task<bool> () =>
         {
            for (var iter = 0; iter < iterationsPerWorker; iter++)
            {
               using var owner = pool.Rent();
               var span = owner.Memory;
               var pattern = (byte)(workerId ^ (iter & 0xFF));
               span.Span.Fill(pattern);

               if ((iter & 0x0F) == 0)
               {
                  await Task.Yield();
               }

               // Verify no other worker overwrote our block while we hold the lease
               for (var idx = 0; idx < span.Length; idx += 511)
               {
                  if (span.Span[idx] != pattern)
                  {
                     throw new InvalidOperationException(
                        $"Memory corruption detected on worker {workerId} at index {idx}: expected 0x{pattern:X2}, found 0x{span.Span[idx]:X2}.");
                  }
               }
            }

            return true;
         })!);
      }

      await Task.WhenAll(tasks);

      await Assert.That(pool.RentedBlocksCount).IsEqualTo(0);
      await Assert.That(pool.CreatedBlocksCount).IsGreaterThan(0);
      await Assert.That(pool.CreatedBlocksCount).IsLessThanOrEqualTo(workerCount);
      await Assert.That(pool.InStoreBlocksCount).IsEqualTo((int)pool.CreatedBlocksCount);
   }

   [Test]
   public async Task Telemetry_RentAndReturn_ShouldRecordTransportMetricsAccurately()
   {
      long createdDelta = 0;
      long rentedDelta = 0;
      long returnedDelta = 0;
      long activeDelta = 0;

      using var listener = new MeterListener();
      listener.InstrumentPublished = (instrument, meterListener) =>
      {
         if (instrument.Meter.Name == TransportMetrics.MeterName)
         {
            meterListener.EnableMeasurementEvents(instrument);
         }
      };
      listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
      {
         switch (instrument.Name)
         {
            case "beskar.transport.memorypool.blocks.created":
               Interlocked.Add(ref createdDelta, measurement);
               break;
            case "beskar.transport.memorypool.blocks.rented":
               Interlocked.Add(ref rentedDelta, measurement);
               break;
            case "beskar.transport.memorypool.blocks.returned":
               Interlocked.Add(ref returnedDelta, measurement);
               break;
            case "beskar.transport.memorypool.blocks.active":
               Interlocked.Add(ref activeDelta, measurement);
               break;
         }
      });
      listener.Start();

      using var pool = new NetworkPinnedBlockMemoryPool();

      // 1st rent creates a new block and rents it
      var owner1 = pool.Rent();
      await Assert.That(Volatile.Read(ref createdDelta)).IsEqualTo(1);
      await Assert.That(Volatile.Read(ref rentedDelta)).IsEqualTo(1);
      await Assert.That(Volatile.Read(ref returnedDelta)).IsEqualTo(0);
      await Assert.That(Volatile.Read(ref activeDelta)).IsEqualTo(1);

      // Disposing returns the block
      owner1.Dispose();
      await Assert.That(Volatile.Read(ref createdDelta)).IsEqualTo(1);
      await Assert.That(Volatile.Read(ref rentedDelta)).IsEqualTo(1);
      await Assert.That(Volatile.Read(ref returnedDelta)).IsEqualTo(1);
      await Assert.That(Volatile.Read(ref activeDelta)).IsEqualTo(0);

      // 2nd rent reuses the block from store (created stays 1, rented becomes 2)
      var owner2 = pool.Rent();
      await Assert.That(Volatile.Read(ref createdDelta)).IsEqualTo(1);
      await Assert.That(Volatile.Read(ref rentedDelta)).IsEqualTo(2);
      await Assert.That(Volatile.Read(ref returnedDelta)).IsEqualTo(1);
      await Assert.That(Volatile.Read(ref activeDelta)).IsEqualTo(1);

      owner2.Dispose();
      await Assert.That(Volatile.Read(ref returnedDelta)).IsEqualTo(2);
      await Assert.That(Volatile.Read(ref activeDelta)).IsEqualTo(0);
   }
}
