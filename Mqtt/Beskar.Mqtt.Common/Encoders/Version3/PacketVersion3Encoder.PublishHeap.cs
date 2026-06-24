using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders.Version3;

public readonly ref partial struct PacketVersion3Encoder
{
   public void WritePublish(PublishOptions options, ushort packetIdentifier = 0)
   {
      var length = CalculateLength(options);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var flags = (byte)((options.Dup ? 8 : 0) | ((int)options.QualityOfService << 1) | (options.Retain ? 1 : 0));
         var remainingLength = CalculateRemainingLength(options);

         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Publish, flags, remainingLength);

         writer.WriteBigEndian((ushort)options.TopicUtf8Bytes.Length);
         if (!options.TopicUtf8Bytes.IsEmpty)
         {
            writer.WriteBytes(options.TopicUtf8Bytes.Span);
         }

         if (options.QualityOfService > QualityOfServiceType.AtMostOnce)
         {
            writer.WriteBigEndian(packetIdentifier);
         }

         if (options.Payload.IsSingleSegment)
         {
            writer.WriteBytes(options.Payload.First.Span);
         }
         else
         {
            foreach (var memory in options.Payload)
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

   private int CalculateLength(PublishOptions options)
   {
      var remainingLength = CalculateRemainingLength(options);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private int CalculateRemainingLength(PublishOptions options)
   {
      var len = 2 + options.TopicUtf8Bytes.Length;

      if (options.QualityOfService > QualityOfServiceType.AtMostOnce)
      {
         len += 2;
      }

      len += (int)options.Payload.Length;
      return len;
   }
}
