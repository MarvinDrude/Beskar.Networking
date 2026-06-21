using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteConnAck(in ConnAckPacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(packet);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.ConnAck, 0, remainingLength);

         var connAckFlags = (byte)(packet.IsSessionPresent ? 1 : 0);
         writer.WriteByte(connAckFlags);
         writer.WriteByte((byte)packet.ReasonCode);

         PacketEncoder.WriteProperties(ref writer, packet.PropertiesBytes);

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static int CalculateLength(in ConnAckPacket packet)
   {
      var remainingLength = CalculateRemainingLength(packet);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private static int CalculateRemainingLength(in ConnAckPacket packet)
   {
      return 2 + CalculatePropertiesLength(packet.PropertiesBytes);
   }
}
