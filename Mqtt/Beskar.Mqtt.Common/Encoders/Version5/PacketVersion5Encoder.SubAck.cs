using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteSubAck(in SubAckPacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(packet);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.SubAck, 0, remainingLength);
         writer.WriteBigEndian(packet.PacketIdentifier);

         PacketEncoder.WriteProperties(ref writer, packet.PropertiesBytes);

         if (packet.ReturnCodesBytes.IsSingleSegment)
         {
            writer.WriteBytes(packet.ReturnCodesBytes.First.Span);
         }
         else
         {
            foreach (var memory in packet.ReturnCodesBytes)
            {
               writer.WriteBytes(memory.Span);
            }
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static int CalculateLength(in SubAckPacket packet)
   {
      var remainingLength = CalculateRemainingLength(packet);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private static int CalculateRemainingLength(in SubAckPacket packet)
   {
      return 2 + CalculatePropertiesLength(packet.PropertiesBytes) + (int)packet.ReturnCodesBytes.Length;
   }
}
