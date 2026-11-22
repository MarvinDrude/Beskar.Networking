using System.Buffers;

namespace Beskar.Networking.Datagram.Reliability;

/// <summary>
/// Wraps a pooled payload buffer rented from <see cref="ArrayPool{T}.Shared"/>.
/// Disposing returns the underlying array to the pool.
/// </summary>
public struct DatagramPayloadLease : IDisposable
{
   private byte[]? _buffer;
   private readonly int _length;

   private DatagramPayloadLease(byte[]? buffer, int length)
   {
      _buffer = buffer;
      _length = length;
   }

   public static DatagramPayloadLease Empty => new(null, 0);

   public readonly int Length => _length;
   public readonly bool IsEmpty => _length == 0;

   public readonly ReadOnlySpan<byte> Span =>
      _buffer is null || _length == 0 ? ReadOnlySpan<byte>.Empty : _buffer.AsSpan(0, _length);

   public readonly ReadOnlyMemory<byte> Memory =>
      _buffer is null || _length == 0 ? ReadOnlyMemory<byte>.Empty : _buffer.AsMemory(0, _length);

   public readonly byte[] ToArray() =>
      _buffer is null || _length == 0 ? [] : _buffer.AsSpan(0, _length).ToArray();

   internal static DatagramPayloadLease CopyFrom(ReadOnlySpan<byte> data)
   {
      if (data.IsEmpty)
      {
         return Empty;
      }

      var rented = ArrayPool<byte>.Shared.Rent(data.Length);
      data.CopyTo(rented);

      return new DatagramPayloadLease(rented, data.Length);
   }

   internal static DatagramPayloadLease WrapRented(byte[] rentedBuffer, int length)
   {
      if (length == 0)
      {
         ArrayPool<byte>.Shared.Return(rentedBuffer);
         return Empty;
      }

      return new DatagramPayloadLease(rentedBuffer, length);
   }

   public void Dispose()
   {
      var buf = Interlocked.Exchange(ref _buffer, null);
      if (buf is not null)
      {
         ArrayPool<byte>.Shared.Return(buf);
      }
   }
}
