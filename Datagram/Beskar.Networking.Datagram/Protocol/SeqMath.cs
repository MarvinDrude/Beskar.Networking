using System.Runtime.CompilerServices;

namespace Beskar.Networking.Datagram.Protocol;

/// <summary>
/// RFC 1982 wrap-around safe sequence number comparisons for 16-bit channel sequences.
/// </summary>
internal static class SeqMath
{
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static bool IsGreater(ushort s1, ushort s2) => unchecked((short)(s1 - s2)) > 0;

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static bool IsGreaterOrEqual(ushort s1, ushort s2) => unchecked((short)(s1 - s2)) >= 0;

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static int Diff(ushort newer, ushort older) => unchecked((short)(newer - older));
}
