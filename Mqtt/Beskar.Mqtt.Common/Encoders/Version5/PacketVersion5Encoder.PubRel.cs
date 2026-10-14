using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WritePubRel(in PubRelPacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(packet);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.PubRel, 2, remainingLength);
         writer.WriteBigEndian(packet.PacketIdentifier);

         if (remainingLength > 2)
         {
            writer.WriteByte((byte)packet.ReasonCode);
            if (remainingLength > 3)
            {
               var propsDataLen = CalculatePropertiesDataLength(packet);
               PacketEncoder.WriteVariableByteInteger(ref writer, (uint)propsDataLen);

               if (!packet.ReasonStringUtf8Bytes.IsEmpty && !HasReasonStringProperty(packet.PropertiesBytes))
               {
                  writer.WriteByte((byte)PropertyIdentifier.ReasonString);
                  writer.WriteBigEndian((ushort)packet.ReasonStringUtf8Bytes.Length);
                  writer.WriteBytes(packet.ReasonStringUtf8Bytes.Span);
               }

               if (!packet.PropertiesBytes.IsEmpty)
               {
                  writer.WriteBytes(packet.PropertiesBytes.Span);
               }
            }
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static int CalculateLength(in PubRelPacket packet)
   {
      var remainingLength = CalculateRemainingLength(packet);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private static int CalculatePropertiesDataLength(in PubRelPacket packet)
   {
      var len = packet.PropertiesBytes.Length;
      if (!packet.ReasonStringUtf8Bytes.IsEmpty && !HasReasonStringProperty(packet.PropertiesBytes))
      {
         len += 3 + packet.ReasonStringUtf8Bytes.Length;
      }
      return len;
   }

   private static int CalculateRemainingLength(in PubRelPacket packet)
   {
      var propsDataLen = CalculatePropertiesDataLength(packet);
      if (packet.ReasonCode == PubRelReasonCode.Success && propsDataLen == 0)
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
