using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteAuth(in AuthPacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(packet);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Auth, 0, remainingLength);

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

   private static int CalculateLength(in AuthPacket packet)
   {
      var remainingLength = CalculateRemainingLength(packet);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private static int CalculateRemainingLength(in AuthPacket packet)
   {
      if (packet.ReasonCode == AuthenticateReasonCode.Success && packet.PropertiesBytes.IsEmpty)
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
