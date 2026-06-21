using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteUnsubscribe(in UnsubscribePacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(packet);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Unsubscribe, 2, remainingLength);
         writer.WriteBigEndian(packet.PacketIdentifier);

         PacketEncoder.WriteProperties(ref writer, packet.PropertiesBytes);

         if (packet.FiltersBytes.IsSingleSegment)
         {
            writer.WriteBytes(packet.FiltersBytes.First.Span);
         }
         else
         {
            foreach (var memory in packet.FiltersBytes)
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

   private static int CalculateLength(in UnsubscribePacket packet)
   {
      var remainingLength = CalculateRemainingLength(packet);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private static int CalculateRemainingLength(in UnsubscribePacket packet)
   {
      return 2 + CalculatePropertiesLength(packet.PropertiesBytes) + (int)packet.FiltersBytes.Length;
   }
}
