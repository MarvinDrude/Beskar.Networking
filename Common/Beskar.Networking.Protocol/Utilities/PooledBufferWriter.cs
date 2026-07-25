using System.Buffers;

namespace Beskar.Networking.Protocol.Utilities;

/// <summary>
/// A high-performance, zero-allocation IBufferWriter implementation backed by ArrayPool.Shared.
/// </summary>
public sealed class PooledBufferWriter(int initialCapacity = 256)
   : IBufferWriter<byte>, IDisposable
{
   private byte[] _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);
   private int _index = 0;

   public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _index);
   public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _index);

   public int WrittenCount => _index;
   public int Capacity => _buffer.Length;

   public int FreeCapacity => _buffer.Length - _index;

   public void Advance(int count)
   {
      if (count < 0 || _index + count > _buffer.Length)
      {
         throw new ArgumentOutOfRangeException(nameof(count));
      }
      _index += count;
   }

   public Memory<byte> GetMemory(int sizeHint = 0)
   {
      CheckAndResizeBuffer(sizeHint);
      return _buffer.AsMemory(_index);
   }

   public Span<byte> GetSpan(int sizeHint = 0)
   {
      CheckAndResizeBuffer(sizeHint);
      return _buffer.AsSpan(_index);
   }

   private void CheckAndResizeBuffer(int sizeHint)
   {
      if (sizeHint <= 0)
      {
         sizeHint = 1;
      }

      if (sizeHint > FreeCapacity)
      {
         var currentLength = _buffer.Length;
         var growBy = Math.Max(sizeHint, currentLength);
         var newSize = currentLength + growBy;

         var newBuffer = ArrayPool<byte>.Shared.Rent(newSize);
         _buffer.AsSpan(0, _index).CopyTo(newBuffer);

         ArrayPool<byte>.Shared.Return(_buffer);
         _buffer = newBuffer;
      }
   }

   public void Dispose()
   {
      if (_buffer.Length <= 0) return;

      var toReturn = _buffer;
      _buffer = [];
      _index = 0;

      ArrayPool<byte>.Shared.Return(toReturn);
   }
}
