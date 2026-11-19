using System.Runtime.CompilerServices;

namespace Beskar.Networking.Datagram.Protocol;

/// <summary>
/// Compact unsigned 7-bit variable-length integer codec operating directly on spans.
/// </summary>
internal static class DatagramVarInt
{
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static int GetEncodedSize(int value)
   {
      var v = (uint)value;
      if (v < 128u) return 1;
      if (v < 16384u) return 2;
      if (v < 2097152u) return 3;
      if (v < 268435456u) return 4;
      return 5;
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static int Write(Span<byte> destination, int value)
   {
      var v = (uint)value;
      var i = 0;

      while (v >= 0x80u)
      {
         destination[i++] = (byte)(v | 0x80u);
         v >>= 7;
      }

      destination[i++] = (byte)v;
      return i;
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static bool TryRead(ReadOnlySpan<byte> source, out int value, out int bytesRead)
   {
      value = 0;
      bytesRead = 0;

      uint result = 0;
      var shift = 0;

      while (bytesRead < source.Length && shift <= 28)
      {
         var b = source[bytesRead++];
         result |= (uint)(b & 0x7F) << shift;

         if ((b & 0x80) == 0)
         {
            if (result > int.MaxValue)
            {
               return false;
            }
            value = (int)result;
            return true;
         }

         shift += 7;
      }

      return false;
   }
}
