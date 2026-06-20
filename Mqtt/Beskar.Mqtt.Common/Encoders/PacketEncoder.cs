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

   }
}
