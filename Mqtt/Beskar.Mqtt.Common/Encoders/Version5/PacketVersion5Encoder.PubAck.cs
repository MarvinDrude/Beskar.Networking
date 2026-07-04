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
               PacketEncoder.WriteProperties(ref writer, new ReadOnlySequence<byte>(packet.PropertiesBytes));
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
      if (packet.ReasonCode == PubAckReasonCode.Success && packet.PropertiesBytes.IsEmpty)
      {
         return 2;
      }

      if (packet.PropertiesBytes.IsEmpty)
      {
         return 3;
      }

      return 3 + CalculatePropertiesLength(new ReadOnlySequence<byte>(packet.PropertiesBytes));
   }
}
