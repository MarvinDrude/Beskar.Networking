using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
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

         var propertiesLength = CalculatePropertiesLength(options);
         PacketEncoder.WriteVariableByteInteger(ref writer, (uint)propertiesLength);

         if (propertiesLength > 0)
         {
            var propEncoder = writer.AsPublishPropertyEncoder();

            if (options.PayloadFormat is not PayloadFormat.Unspecified)
            {
               propEncoder.WritePayloadFormatIndicator(options.PayloadFormat);
            }

            if (options.MessageExpiryInterval.HasValue && options.MessageExpiryInterval.Value != 0)
            {
               propEncoder.WriteMessageExpiryInterval(options.MessageExpiryInterval.Value);
            }

            if (!options.ContentTypeUtf8Bytes.IsEmpty)
            {
               propEncoder.WriteContentType(options.ContentTypeUtf8Bytes.Span);
            }

            if (!options.ResponseTopicUtf8Bytes.IsEmpty)
            {
               propEncoder.WriteResponseTopic(options.ResponseTopicUtf8Bytes.Span);
            }

            if (!options.CorrelationData.IsEmpty)
            {
               propEncoder.WriteCorrelationData(options.CorrelationData.Span);
            }

            if (options.TopicAlias.HasValue && options.TopicAlias.Value != 0)
            {
               propEncoder.WriteTopicAlias(options.TopicAlias.Value);
            }

            if (options.SubscriptionIdentifiers.Count > 0)
            {
               var enumerator = options.SubscriptionIdentifiers.GetEnumerator();
               while (enumerator.MoveNext())
               {
                  propEncoder.WriteSubscriptionIdentifier(enumerator.Current);
               }
            }

            if (options.UserProperties.Count > 0)
            {
               var enumerator = options.UserProperties.GetEnumerator();
               while (enumerator.MoveNext())
               {
                  var prop = enumerator.Current;
                  propEncoder.WriteUserProperty(prop.KeyUtf8Bytes, prop.ValueBytes);
               }
            }

            writer = propEncoder.Encoder.Writer;
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

      var propertiesLength = CalculatePropertiesLength(options);
      len += PacketEncoder.GetVariableByteIntegerLength(propertiesLength) + propertiesLength;
      len += (int)options.Payload.Length;

      return len;
   }

   private static int CalculatePropertiesLength(PublishOptions options)
   {
      var len = 0;

      if (options.PayloadFormat is not PayloadFormat.Unspecified)
      {
         len += 2;
      }

      if (options.MessageExpiryInterval.HasValue && options.MessageExpiryInterval.Value != 0)
      {
         len += 5;
      }

      if (!options.ContentTypeUtf8Bytes.IsEmpty)
      {
         len += 3 + options.ContentTypeUtf8Bytes.Length;
      }

      if (!options.ResponseTopicUtf8Bytes.IsEmpty)
      {
         len += 3 + options.ResponseTopicUtf8Bytes.Length;
      }

      if (!options.CorrelationData.IsEmpty)
      {
         len += 3 + options.CorrelationData.Length;
      }

      if (options.TopicAlias.HasValue && options.TopicAlias.Value != 0)
      {
         len += 3;
      }

      len += options.SubscriptionIdentifiers.ByteCount;
      len += options.UserProperties.ByteCount;

      return len;
   }
}
