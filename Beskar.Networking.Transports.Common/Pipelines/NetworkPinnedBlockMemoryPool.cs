using System.Buffers;
using System.Runtime.InteropServices;
using System.Threading.Channels;

namespace Beskar.Networking.Transports.Common.Pipelines;

/// <summary>
/// Provides a memory pool implementation that manages pinned blocks of memory,
/// suitable for scenarios requiring stable memory addresses such as interop or
/// high-performance IO operations.
/// </summary>
/// <remarks>
/// This class manages memory allocation and reuse through a pool of memory blocks. The
/// pinned memory blocks are designed to prevent garbage collection from relocating the
/// memory, ensuring that the memory address remains stable for the lifetime of the block.
/// </remarks>
public sealed class NetworkPinnedBlockMemoryPool : MemoryPool<byte>
{
   /// <summary>
   /// The size of a block. 4096 is chosen because most operating systems use 4k pages.
   /// </summary>
   private const int BlockSizeInner = 4096;

   /// <summary>
   /// Represents a constant value used to indicate that any size of memory buffer can be requested.
   /// This is typically used as a default value in memory allocation scenarios to allow for
   /// flexibility when no specific size is required.
   /// </summary>
   private const int AnySize = -1;

   /// <summary>
   /// Gets the maximum buffer size that can be allocated from the memory pool.
   /// This value represents the upper limit on the size of a single buffer that
   /// the pool can provide.
   /// </summary>
   public override int MaxBufferSize => BlockSizeInner;

   /// <summary>
   /// Represents the size of a memory block used in the memory pool.
   /// The value is defined based on common system page sizes to optimize memory usage and performance.
   /// </summary>
   public static int BlockSize => BlockSizeInner;

   /// <summary>
   /// A channel used to manage the pool of reusable memory blocks.
   /// Acts as the core mechanism for allocating and returning blocks within the memory pool.
   /// </summary>
   private readonly Channel<NetworkMemoryPoolBlock> _blocks = Channel.CreateUnbounded<NetworkMemoryPoolBlock>();

   /// <summary>
   /// Indicates whether the memory pool instance has been disposed of.
   /// When true, the instance can no longer be used for memory allocation or other operations.
   /// </summary>
   private bool _isDisposed;

   private long _createdBlocksCount;

   public long CreatedBlocksCount => Volatile.Read(ref _createdBlocksCount);
   public int InStoreBlocksCount => _blocks.Reader.Count;
   public long RentedBlocksCount => CreatedBlocksCount - InStoreBlocksCount;

   /// <summary>
   /// A synchronization lock used to safely control access to resources during the pool's
   /// disposal process, ensuring thread-safe operations when releasing memory blocks.
   /// </summary>
   private readonly Lock _disposeSync = new();

   public override IMemoryOwner<byte> Rent(int size = AnySize)
   {
      ArgumentOutOfRangeException.ThrowIfGreaterThan(size, BlockSizeInner, nameof(size));
      ObjectDisposedException.ThrowIf(_isDisposed, this);

      if (_blocks.Reader.TryRead(out var block))
      {
         return block;
      }

      Interlocked.Increment(ref _createdBlocksCount);
      return new NetworkMemoryPoolBlock(this, BlockSize);
   }

   /// <summary>
   /// Returns a memory block to the pinned memory pool for reuse.
   /// </summary>
   /// <param name="block">
   /// The memory block to return to the pool. The block must have been originally allocated
   /// from the same memory pool instance to maintain proper pool behavior.
   /// </param>
   internal void Return(NetworkMemoryPoolBlock block)
   {
      if (!_isDisposed)
      {
         _blocks.Writer.TryWrite(block);
      }
   }

   /// <summary>
   /// Releases the resources used by the memory pool instance.
   /// This includes disposing of all resources and clearing memory blocks from the pool.
   /// </summary>
   /// <param name="disposing">
   /// A boolean value indicating whether the method is being called explicitly.
   /// If true, managed and unmanaged resources will be released. If false, only
   /// unmanaged resources will be released.
   /// </param>
   protected override void Dispose(bool disposing)
   {
      if (_isDisposed)
      {
         return;
      }

      lock (_disposeSync)
      {
         _isDisposed = true;

         if (!disposing)
         {
            return;
         }

         while (_blocks.Reader.TryRead(out _))
         {

         }
      }
   }

}

/// <summary>
/// Represents a block of memory that is allocated and managed by a pinned memory pool.
/// </summary>
internal sealed class NetworkMemoryPoolBlock
   : IMemoryOwner<byte>
{
   /// <summary>
   /// Represents a block of memory that is allocated and managed by a pinned memory pool.
   /// </summary>
   /// <remarks>
   /// The memory block is pinned to avoid movement by the garbage collector, making it suitable
   /// for use in scenarios that require stable memory addresses, such as interop or high-performance
   /// IO operations.
   /// </remarks>
   internal NetworkMemoryPoolBlock(NetworkPinnedBlockMemoryPool pool, int length)
   {
      Pool = pool;

      var pinnedArray = GC.AllocateUninitializedArray<byte>(length, pinned: true);
      Memory = MemoryMarshal.CreateFromPinnedArray(pinnedArray, 0, pinnedArray.Length);
   }

   /// <summary>
   /// Gets the instance of the pinned memory pool that manages the current memory block.
   /// </summary>
   public NetworkPinnedBlockMemoryPool Pool { get; }

   /// <summary>
   /// Represents a contiguous region of memory that is allocated and pinned in memory, preventing the garbage collector
   /// from relocating it. This property is used to access the memory block managed by the memory pool.
   /// </summary>
   public Memory<byte> Memory { get; }

   /// <summary>
   /// Releases the block of memory back to the originating pinned memory pool.
   /// </summary>
   /// <remarks>
   /// This method should be called when the memory block is no longer needed to return
   /// it to the pool for reuse. Failure to dispose properly could result in memory
   /// not being released to the pool, affecting overall performance and memory usage.
   /// </remarks>
   public void Dispose()
   {
      Pool.Return(this);
   }
}
