using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WritePublish(in PublishPacket packet)
   {
      var existingMask = CollectPresentProperties(packet.PropertiesBytes);
      var propsDataLen = CalculatePublishPropertiesDataLength(in packet, existingMask);
      var remainingLength = CalculateRemainingLength(in packet, propsDataLen);
      var length = PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var flags = (byte)((packet.Dup ? 8 : 0) | ((int)packet.QualityOfService << 1) | (packet.Retain ? 1 : 0));

         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Publish, flags, remainingLength);
         PacketEncoder.WriteSequence(ref writer, packet.TopicUtf8Bytes);

         if (packet.QualityOfService > QualityOfServiceType.AtMostOnce)
         {
            writer.WriteBigEndian(packet.PacketIdentifier);
         }

         PacketEncoder.WriteVariableByteInteger(ref writer, (uint)propsDataLen);
         if (propsDataLen > 0)
         {
            var propEncoder = writer.AsPublishPropertyEncoder();
            try
            {
               if (packet.PayloadFormat != PayloadFormat.Unspecified
                   && !HasProperty(existingMask, PropertyIdentifier.PayloadFormatIndicator))
               {
                  propEncoder.WritePayloadFormatIndicator(packet.PayloadFormat);
               }

               if (packet.MessageExpiryInterval != 0
                   && !HasProperty(existingMask, PropertyIdentifier.MessageExpiryInterval))
               {
                  propEncoder.WriteMessageExpiryInterval(packet.MessageExpiryInterval);
               }

               if (packet.TopicAlias != 0
                   && !HasProperty(existingMask, PropertyIdentifier.TopicAlias))
               {
                  propEncoder.WriteTopicAlias(packet.TopicAlias);
               }

               if (!packet.ResponseTopicUtf8Bytes.IsEmpty
                   && !HasProperty(existingMask, PropertyIdentifier.ResponseTopic))
               {
                  propEncoder.Encoder.Write(PropertyIdentifier.ResponseTopic, packet.ResponseTopicUtf8Bytes);
               }

               if (!packet.CorrelationDataBytes.IsEmpty
                   && !HasProperty(existingMask, PropertyIdentifier.CorrelationData))
               {
                  propEncoder.Encoder.Write(PropertyIdentifier.CorrelationData, packet.CorrelationDataBytes);
               }

               if (!packet.ContentTypeUtf8Bytes.IsEmpty
                   && !HasProperty(existingMask, PropertyIdentifier.ContentType))
               {
                  propEncoder.Encoder.Write(PropertyIdentifier.ContentType, packet.ContentTypeUtf8Bytes);
               }

               if (packet.SubscriptionIdentifier != 0
                   && !HasProperty(existingMask, PropertyIdentifier.SubscriptionIdentifier))
               {
                  propEncoder.WriteSubscriptionIdentifier(packet.SubscriptionIdentifier);
               }
            }
            finally
            {
               writer = propEncoder.Encoder.Writer;
            }

            PacketEncoder.WriteRawSequence(ref writer, packet.PropertiesBytes);
         }

         PacketEncoder.WriteRawSequence(ref writer, packet.Payload);

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   /// <summary>
   /// Calculates the exact encoded byte length of an MQTT 5.0 PUBLISH packet.
   /// </summary>
   internal static int CalculatePublishPacketLength(in PublishPacket packet)
   {
      var existingMask = CollectPresentProperties(packet.PropertiesBytes);
      var propsDataLen = CalculatePublishPropertiesDataLength(in packet, existingMask);
      var remainingLength = CalculateRemainingLength(in packet, propsDataLen);

      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private static int CalculateRemainingLength(in PublishPacket packet, int propsDataLen)
   {
      var len = 2 + (int)packet.TopicUtf8Bytes.Length;

      if (packet.QualityOfService > QualityOfServiceType.AtMostOnce)
      {
         len += 2;
      }

      len += PacketEncoder.GetVariableByteIntegerLength(propsDataLen) + propsDataLen;
      len += (int)packet.Payload.Length;

      return len;
   }

   private static int CalculatePublishPropertiesDataLength(in PublishPacket packet, ulong existingMask)
   {
      var len = (int)packet.PropertiesBytes.Length;

      if (packet.PayloadFormat != PayloadFormat.Unspecified
          && !HasProperty(existingMask, PropertyIdentifier.PayloadFormatIndicator))
      {
         len += 2;
      }

      if (packet.MessageExpiryInterval != 0
          && !HasProperty(existingMask, PropertyIdentifier.MessageExpiryInterval))
      {
         len += 5;
      }

      if (packet.TopicAlias != 0
          && !HasProperty(existingMask, PropertyIdentifier.TopicAlias))
      {
         len += 3;
      }

      if (!packet.ResponseTopicUtf8Bytes.IsEmpty
          && !HasProperty(existingMask, PropertyIdentifier.ResponseTopic))
      {
         len += 3 + (int)packet.ResponseTopicUtf8Bytes.Length;
      }

      if (!packet.CorrelationDataBytes.IsEmpty
          && !HasProperty(existingMask, PropertyIdentifier.CorrelationData))
      {
         len += 3 + (int)packet.CorrelationDataBytes.Length;
      }

      if (!packet.ContentTypeUtf8Bytes.IsEmpty
          && !HasProperty(existingMask, PropertyIdentifier.ContentType))
      {
         len += 3 + (int)packet.ContentTypeUtf8Bytes.Length;
      }

      if (packet.SubscriptionIdentifier != 0
          && !HasProperty(existingMask, PropertyIdentifier.SubscriptionIdentifier))
      {
         len += 1 + PacketEncoder.GetVariableByteIntegerLength((int)packet.SubscriptionIdentifier);
      }

      return len;
   }
}
