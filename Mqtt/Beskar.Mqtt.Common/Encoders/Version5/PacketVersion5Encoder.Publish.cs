using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WritePublish(in PublishPacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var flags = (byte)((packet.Dup ? 8 : 0) | ((int)packet.QualityOfService << 1) | (packet.Retain ? 1 : 0));
         var remainingLength = CalculateRemainingLength(packet);

         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Publish, flags, remainingLength);
         PacketEncoder.WriteSequence(ref writer, packet.TopicUtf8Bytes);

         if (packet.QualityOfService > QualityOfServiceType.AtMostOnce)
         {
            writer.WriteBigEndian(packet.PacketIdentifier);
         }

         PacketEncoder.WriteProperties(ref writer, packet.PropertiesBytes);

         if (packet.Payload.IsSingleSegment)
         {
            writer.WriteBytes(packet.Payload.First.Span);
         }
         else
         {
            foreach (var memory in packet.Payload)
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

   private int CalculateLength(in PublishPacket packet)
   {
      var remainingLength = CalculateRemainingLength(packet);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private int CalculateRemainingLength(in PublishPacket packet)
   {
      var len = 2 + (int)packet.TopicUtf8Bytes.Length;

      if (packet.QualityOfService > QualityOfServiceType.AtMostOnce)
      {
         len += 2;
      }

      len += CalculatePropertiesLength(packet.PropertiesBytes);
      len += (int)packet.Payload.Length;

      return len;
   }
}
