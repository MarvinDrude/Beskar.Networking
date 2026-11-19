using System.Buffers.Binary;
using System.Runtime.CompilerServices;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;

namespace Beskar.Networking.Datagram.Protocol;

internal readonly ref struct DecodedChannelMessage
{
   public byte ChannelId { get; init; }
   public bool IsFragment { get; init; }
   public ushort ChannelSeq { get; init; }

   public ushort FragGroupId { get; init; }
   public ushort FragIndex { get; init; }
   public ushort FragCount { get; init; }

   public ReadOnlySpan<byte> Payload { get; init; }
}

/// <summary>
/// Encodes and decodes individual channel messages coalesced inside a <see cref="DatagramPacketKind.Data"/> packet.
/// </summary>
internal static class DatagramMessageCodec
{
   public const int MaxChannelMessageOverhead = 1 + 2 + 6 + 3; // MsgHeader(1) + Seq(2) + Frag(6) + VarInt(<=3)

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static bool HasChannelSeq(DatagramDeliveryMode mode) =>
      mode != DatagramDeliveryMode.UnreliableUnordered;

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static int GetEncodedSize(
      DatagramDeliveryMode mode,
      bool isFragment,
      int payloadLength)
   {
      var size = 1; // ChannelId (6 bits) + IsFragment (1 bit)

      if (HasChannelSeq(mode)) size += 2;
      if (isFragment) size += 6;

      size += DatagramVarInt.GetEncodedSize(payloadLength);
      size += payloadLength;

      return size;
   }

   public static int Write(
      Span<byte> destination,
      byte channelId,
      DatagramDeliveryMode mode,
      ushort channelSeq,
      bool isFragment,
      ushort fragGroupId,
      ushort fragIndex,
      ushort fragCount,
      ReadOnlySpan<byte> payload)
   {
      var offset = 0;
      destination[offset++] = (byte)((channelId & 0x3F) | (isFragment ? 0x40 : 0x00));

      if (HasChannelSeq(mode))
      {
         BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(offset, 2), channelSeq);
         offset += 2;
      }

      if (isFragment)
      {
         BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(offset, 2), fragGroupId);
         BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(offset + 2, 2), fragIndex);
         BinaryPrimitives.WriteUInt16LittleEndian(destination.Slice(offset + 4, 2), fragCount);
         offset += 6;
      }

      offset += DatagramVarInt.Write(destination[offset..], payload.Length);
      payload.CopyTo(destination.Slice(offset, payload.Length));
      offset += payload.Length;

      return offset;
   }

   public static bool TryRead(
      ReadOnlySpan<byte> source,
      DatagramChannelConfig?[] channels,
      out DecodedChannelMessage message,
      out int bytesConsumed)
   {
      message = default;
      bytesConsumed = 0;

      if (source.Length < 2)
      {
         return false;
      }

      var offset = 0;
      var headerByte = source[offset++];
      var channelId = (byte)(headerByte & 0x3F);
      var isFragment = (headerByte & 0x40) != 0;

      var config = channels[channelId];
      if (config is null)
      {
         return false;
      }

      ushort channelSeq = 0;
      if (HasChannelSeq(config.DeliveryMode))
      {
         if (offset + 2 > source.Length) return false;
         channelSeq = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(offset, 2));
         offset += 2;
      }

      ushort fragGroupId = 0;
      ushort fragIndex = 0;
      ushort fragCount = 0;
      if (isFragment)
      {
         if (offset + 6 > source.Length) return false;
         fragGroupId = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(offset, 2));
         fragIndex = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(offset + 2, 2));
         fragCount = BinaryPrimitives.ReadUInt16LittleEndian(source.Slice(offset + 4, 2));
         offset += 6;

         if (fragCount == 0 || fragIndex >= fragCount)
         {
            return false;
         }
      }

      if (!DatagramVarInt.TryRead(source[offset..], out var payloadLen, out var varBytes) || payloadLen < 0)
      {
         return false;
      }

      offset += varBytes;
      if (offset + payloadLen > source.Length)
      {
         return false;
      }

      message = new DecodedChannelMessage
      {
         ChannelId = channelId,
         IsFragment = isFragment,
         ChannelSeq = channelSeq,
         FragGroupId = fragGroupId,
         FragIndex = fragIndex,
         FragCount = fragCount,
         Payload = source.Slice(offset, payloadLen)
      };

      bytesConsumed = offset + payloadLen;
      return true;
   }
}
