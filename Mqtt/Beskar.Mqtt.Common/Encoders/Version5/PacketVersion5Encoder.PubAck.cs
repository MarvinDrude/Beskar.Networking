using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WritePubAck(in PubAckPacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(packet);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.PubAck, 0, remainingLength);
         writer.WriteBigEndian(packet.PacketIdentifier);

         if (remainingLength > 2)
         {
            writer.WriteByte((byte)packet.ReasonCode);
            if (remainingLength > 3)
            {
               var propsDataLen = CalculateReasonPropertiesDataLength(packet.ReasonStringUtf8Bytes, packet.PropertiesBytes);
               WriteReasonProperties(ref writer, packet.ReasonStringUtf8Bytes, packet.PropertiesBytes, propsDataLen);
            }
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static int CalculateLength(in PubAckPacket packet)
   {
      var remainingLength = CalculateRemainingLength(packet);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private static int CalculateRemainingLength(in PubAckPacket packet)
   {
      var propsDataLen = CalculateReasonPropertiesDataLength(packet.ReasonStringUtf8Bytes, packet.PropertiesBytes);
      if (packet.ReasonCode == PubAckReasonCode.Success && propsDataLen == 0)
      {
         return 2;
      }

      if (propsDataLen == 0)
      {
         return 3;
      }

      return 3 + PacketEncoder.GetVariableByteIntegerLength(propsDataLen) + propsDataLen;
   }
}
