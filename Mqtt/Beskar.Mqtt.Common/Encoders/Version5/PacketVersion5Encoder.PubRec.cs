using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WritePubRec(in PubRecPacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(packet);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.PubRec, 0, remainingLength);
         writer.WriteBigEndian(packet.PacketIdentifier);

         if (remainingLength > 2)
         {
            writer.WriteByte((byte)packet.ReasonCode);
            if (remainingLength > 3)
            {
               PacketEncoder.WriteProperties(ref writer, packet.PropertiesBytes);
            }
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static int CalculateLength(in PubRecPacket packet)
   {
      var remainingLength = CalculateRemainingLength(packet);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private static int CalculateRemainingLength(in PubRecPacket packet)
   {
      if (packet.ReasonCode == PubRecReasonCode.Success && packet.PropertiesBytes.IsEmpty)
      {
         return 2;
      }

      if (packet.PropertiesBytes.IsEmpty)
      {
         return 3;
      }

      return 3 + CalculatePropertiesLength(packet.PropertiesBytes);
   }
}
