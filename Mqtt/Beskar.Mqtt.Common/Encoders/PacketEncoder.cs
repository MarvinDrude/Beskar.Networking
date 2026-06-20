using System.Buffers;
using System.Runtime.CompilerServices;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders;

/// <summary>
/// Helper methods for encoding
/// </summary>
public static class PacketEncoder
{
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static int GetVariableByteIntegerLength(int value)
   {
      return value switch
      {
         < 128 => 1,
         < 16384 => 2,
         < 2097152 => 3,
         _ => 4
      };
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public static int CalculateFixedHeaderLength(int remainingLength)
   {
      return 1 + GetVariableByteIntegerLength(remainingLength);
   }

   public static void WriteFixedHeader(
      ref ByteWriter writer, MqttPacketType packetType, byte flags, int remainingLength)
   {
      writer.WriteByte((byte)(((int)packetType << 4) | (flags & 0x0F)));
      var value = remainingLength;

      do
      {
         var encodedByte = (byte)(value & 0x7F);
         value >>= 7;

         if (value > 0)
         {
            encodedByte |= 0x80;
         }

         writer.WriteByte(encodedByte);
      }
      while (value > 0);
   }

   public static void WriteSequence(
      ref ByteWriter writer, ReadOnlySequence<byte> sequence)
   {
      var length = (ushort)sequence.Length;

      writer.WriteBigEndian(length);
      if (length == 0) return;

      if (sequence.IsSingleSegment)
      {
         writer.WriteBytes(sequence.First.Span);
      }
      else
      {
         foreach (var memory in sequence)
         {
            writer.WriteBytes(memory.Span);
         }
      }
   }
}
