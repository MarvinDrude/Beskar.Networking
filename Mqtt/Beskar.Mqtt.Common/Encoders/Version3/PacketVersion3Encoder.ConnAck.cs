using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version3;

public readonly ref partial struct PacketVersion3Encoder
{
   public void WriteConnAck(in ConnAckPacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.ConnAck, 0, 2);

         byte connAckFlags = 0;
         if (_protocolVersion is not MqttProtocolVersion.V31 && packet.SessionPresent)
         {
            connAckFlags = 1;
         }

         writer.WriteByte(connAckFlags);
         writer.WriteByte((byte)packet.ReturnCode);

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static int CalculateLength(in ConnAckPacket packet)
   {
      const int remainingLength = 2; // always fixed
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }
}
