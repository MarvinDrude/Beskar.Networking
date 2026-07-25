using System.Buffers;
using System.Numerics;
using System.Runtime.CompilerServices;

namespace Beskar.Networking.Protocol.Utilities;

/// <summary>
/// High-performance generic variable-length integer encoding and decoding utilities.
/// </summary>
public static class VarNumber
{
   /// <summary>
   /// Calculates the encoded byte length of a variable-length integer.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static int GetEncodedLength<T>(T value)
      where T : struct, IBinaryInteger<T>
   {
      var val = ulong.CreateTruncating(value);

      return val switch
      {
         < 128 => 1,
         < 16384 => 2,
         < 2097152 => 3,
         < 268435456 => 4,
         < 34359738368UL => 5,
         < 4398046511104UL => 6,
         < 562949953421312UL => 7,
         < 72057594037927936UL => 8,
         _ => val < 9223372036854775808UL ? 9 : 10
      };
   }

   /// <summary>
   /// Writes a variable-length integer into a byte destination span.
   /// Returns the number of bytes written.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static int Write<T>(Span<byte> destination, T value)
      where T : struct, IBinaryInteger<T>
   {
      var val = ulong.CreateTruncating(value);
      var bytesWritten = 0;

      do
      {
         var b = (byte)(val & 0x7F);
         val >>= 7;

         if (val > 0) b |= 0x80;
         destination[bytesWritten++] = b;
      }
      while (val > 0);

      return bytesWritten;
   }

   /// <summary>
   /// Tries to read a variable-length integer from a SequenceReader.
   /// </summary>
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static bool TryRead<T>(ref SequenceReader<byte> reader, out T result) where T : struct, IBinaryInteger<T>
   {
      result = default;
      ulong val = 0;
      var shift = 0;

      while (shift < 70)
      {
         if (!reader.TryRead(out var b)) return false;
         val |= (ulong)(b & 0x7F) << shift;

         if ((b & 0x80) == 0)
         {
            result = T.CreateTruncating(val);
            return true;
         }

         shift += 7;
      }

      return false;
   }
}
