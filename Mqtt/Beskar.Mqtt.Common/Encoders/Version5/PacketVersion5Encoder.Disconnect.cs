using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteDisconnect(in DisconnectPacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(packet);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Disconnect, 0, remainingLength);

         if (remainingLength > 0)
         {
            writer.WriteByte((byte)packet.ReasonCode);
            if (remainingLength > 1)
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

   private static int CalculateLength(in DisconnectPacket packet)
   {
      var remainingLength = CalculateRemainingLength(packet);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private static int CalculateRemainingLength(in DisconnectPacket packet)
   {
      if (packet.ReasonCode == DisconnectReasonCode.NormalDisconnection && packet.PropertiesBytes.IsEmpty)
      {
         return 0;
      }

      if (packet.PropertiesBytes.IsEmpty)
      {
         return 1;
      }

      return 1 + CalculatePropertiesLength(packet.PropertiesBytes);
   }
}
