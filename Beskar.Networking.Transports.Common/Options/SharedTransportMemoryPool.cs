using System.Buffers;
using Beskar.Memory.Pools;

namespace Beskar.Networking.Transports.Common.Options;

/// <summary>
/// Provides a process-wide shared registry of pinned memory pools for all transport queues.
/// Multiple listeners and clients in the same process share the same pool instances,
/// reducing total unmanaged memory by avoiding duplicate pool allocations per transport.
/// </summary>
public static class SharedTransportMemoryPool
{
   /// <summary>
   /// The number of shared memory pools. Matches typical IO queue counts.
   /// </summary>
   public static int PoolCount { get; } = Math.Min(Environment.ProcessorCount, 12);

   private static readonly PinnedBlockMemoryPool[] Pools;
   private static ulong _currentIndex;

   static SharedTransportMemoryPool()
   {
      Pools = new PinnedBlockMemoryPool[PoolCount];

      for (var i = 0; i < PoolCount; i++)
      {
         Pools[i] = new PinnedBlockMemoryPool();
      }
   }

   /// <summary>
   /// Gets the next memory pool from the shared registry using round-robin assignment.
   /// Thread-safe via atomic increment.
   /// </summary>
   public static PinnedBlockMemoryPool GetNext()
   {
      var index = Interlocked.Increment(ref _currentIndex) % (ulong)PoolCount;
      return Pools[index];
   }

   /// <summary>
   /// Gets the aggregated stats of all shared memory pools.
   /// </summary>
   public static (long Created, int InStore, long Rented) GetStats()
   {
      long created = 0;
      var inStore = 0;
      long rented = 0;

      foreach (var pool in Pools)
      {
         created += pool.CreatedBlocksCount;
         inStore += pool.InStoreBlocksCount;
         rented += pool.RentedBlocksCount;
      }

      return (created, inStore, rented);
   }
}
