using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version3;

public readonly ref partial struct PacketVersion3Encoder
{
   public void WriteSubscribe(in SubscribePacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(packet);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Subscribe, 2, remainingLength);

         writer.WriteBigEndian(packet.PacketIdentifier);

         foreach (var memory in packet.FiltersBytes)
         {
            writer.WriteBytes(memory.Span);
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static int CalculateLength(in SubscribePacket packet)
   {
      var remainingLength = CalculateRemainingLength(packet);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private static int CalculateRemainingLength(in SubscribePacket packet)
   {
      return 2 + (int)packet.FiltersBytes.Length;
   }
}
