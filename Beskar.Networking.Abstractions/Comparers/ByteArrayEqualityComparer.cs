namespace Beskar.Networking.Abstractions.Comparers;

public sealed class ByteArrayEqualityComparer
   : IEqualityComparer<byte[]>,
     IAlternateEqualityComparer<ReadOnlySpan<byte>, byte[]>,
     IAlternateEqualityComparer<ReadOnlyMemory<byte>, byte[]>,
     IAlternateEqualityComparer<Memory<byte>, byte[]>
{
   public static readonly ByteArrayEqualityComparer Instance = new();

   public bool Equals(byte[]? x, byte[]? y)
   {
      if (ReferenceEquals(x, y)) return true;
      if (x is null || y is null) return false;
      return x.AsSpan().SequenceEqual(y);
   }

   public int GetHashCode(byte[] obj)
   {
      return GetHashCode(obj.AsSpan());
   }

   public bool Equals(ReadOnlySpan<byte> alternate, byte[] other)
   {
      return alternate.SequenceEqual(other);
   }

   public int GetHashCode(ReadOnlySpan<byte> alternate)
   {
      var hashCode = new HashCode();
      hashCode.AddBytes(alternate);

      return hashCode.ToHashCode();
   }

   public byte[] Create(ReadOnlySpan<byte> alternate)
   {
      return [.. alternate];
   }

   public bool Equals(ReadOnlyMemory<byte> alternate, byte[] other)
   {
      return alternate.Span.SequenceEqual(other);
   }

   public int GetHashCode(ReadOnlyMemory<byte> alternate)
   {
      return GetHashCode(alternate.Span);
   }

   public byte[] Create(ReadOnlyMemory<byte> alternate)
   {
      return [.. alternate.Span];
   }

   public bool Equals(Memory<byte> alternate, byte[] other)
   {
      return alternate.Span.SequenceEqual(other);
   }

   public int GetHashCode(Memory<byte> alternate)
   {
      return GetHashCode(alternate.Span);
   }

   public byte[] Create(Memory<byte> alternate)
   {
      return [.. alternate.Span];
   }
}
