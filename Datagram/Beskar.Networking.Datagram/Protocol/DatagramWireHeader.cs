using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Beskar.Networking.Datagram.Enums;

namespace Beskar.Networking.Datagram.Protocol;

/// <summary>
/// Fixed 21-byte wire header prepended to every UDP datagram.
/// Also passed directly as Additional Authenticated Data (AAD) when AEAD encryption is active.
/// </summary>
internal readonly record struct DatagramWireHeader(
   DatagramPacketKind Kind,
   byte Version,
   bool IsEncrypted,
   uint SessionToken,
   uint PacketSeq,
   uint AckSeq,
   ulong AckBitfield)
{
   public const int Size = 21;
   public const byte CurrentVersion = 1;

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public void Write(Span<byte> destination)
   {
      var flags = (byte)(((byte)Kind & 0x0F)
         | ((Version & 0x03) << 4)
         | (IsEncrypted ? 0x40 : 0x00));

      destination[0] = flags;

      BinaryPrimitives.WriteUInt32LittleEndian(destination[1..5], SessionToken);
      BinaryPrimitives.WriteUInt32LittleEndian(destination[5..9], PacketSeq);
      BinaryPrimitives.WriteUInt32LittleEndian(destination[9..13], AckSeq);
      BinaryPrimitives.WriteUInt64LittleEndian(destination[13..21], AckBitfield);
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static bool TryRead(ReadOnlySpan<byte> source, out DatagramWireHeader header)
   {
      if (source.Length < Size)
      {
         header = default;
         return false;
      }

      var flags = source[0];
      var kind = (DatagramPacketKind)(flags & 0x0F);
      var version = (byte)((flags >> 4) & 0x03);
      var isEncrypted = (flags & 0x40) != 0;

      if (version != CurrentVersion || !Enum.IsDefined(kind))
      {
         header = default;
         return false;
      }

      var sessionToken = BinaryPrimitives.ReadUInt32LittleEndian(source[1..5]);
      var packetSeq = BinaryPrimitives.ReadUInt32LittleEndian(source[5..9]);
      var ackSeq = BinaryPrimitives.ReadUInt32LittleEndian(source[9..13]);
      var ackBitfield = BinaryPrimitives.ReadUInt64LittleEndian(source[13..21]);

      header = new DatagramWireHeader(
         kind,
         version,
         isEncrypted,
         sessionToken,
         packetSeq,
         ackSeq,
         ackBitfield);

      return true;
   }
}
