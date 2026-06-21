using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Mqtt.Protocol.Validators.Version5;

namespace Beskar.Mqtt.Common.Parsers.Version5;

public readonly ref partial struct PacketVersion5Parser
{
   /// <summary>
   /// Parses the PUBLISH packet variable header and payload.
   /// </summary>
   private static Result<PacketDispatchResult, StringError> TryParsePublishPacket(
      ref RawPacket rawPacket,
      ref PublishPacket packet)
   {
      var initialConsumed = rawPacket.Reader.Consumed;

      packet.Dup = (rawPacket.FixedHeader & 0x08) > 0;
      packet.QualityOfService = (QualityOfServiceType)((rawPacket.FixedHeader & 0x06) >> 1);
      packet.Retain = (rawPacket.FixedHeader & 0x01) > 0;

      if (!rawPacket.Reader.TryReadRawString(out packet.TopicUtf8Bytes))
      {
         return new StringError("Could not read topic.");
      }

      if (packet.QualityOfService > QualityOfServiceType.AtMostOnce)
      {
         if (!rawPacket.Reader.TryReadUInt16BigEndian(out packet.PacketIdentifier))
         {
            return new StringError("Could not read packet identifier.");
         }
      }

      if (!rawPacket.Reader.TryReadProperties(out packet.PropertiesBytes))
      {
         return new StringError("Could not read properties.");
      }

      var enumerator = packet.GetProperties();

      while (enumerator.MoveNext())
      {
         switch (enumerator.Current.Identifier)
         {
            case PropertyIdentifier.ResponseTopic:
               packet.ResponseTopicUtf8Bytes = enumerator.Current.AsResponseTopic();
               break;
            case PropertyIdentifier.PayloadFormatIndicator:
               packet.PayloadFormat = enumerator.Current.AsPayloadFormat();
               break;
            case PropertyIdentifier.MessageExpiryInterval:
               packet.MessageExpiryInterval = enumerator.Current.AsMessageExpiryInterval();
               break;
            case PropertyIdentifier.TopicAlias:
               packet.TopicAlias = enumerator.Current.AsTopicAlias();
               break;
            case PropertyIdentifier.CorrelationData:
               packet.CorrelationDataBytes = enumerator.Current.AsCorrelationData();
               break;
            case PropertyIdentifier.ContentType:
               packet.ContentTypeUtf8Bytes = enumerator.Current.AsContentType();
               break;
            case PropertyIdentifier.SubscriptionIdentifier:
               var subIdResult = enumerator.Current.AsSubscriptionIdentifier();
               if (subIdResult.Failed) return subIdResult.Error;

               if (packet.SubscriptionIdentifier == 0)
               {
                  packet.SubscriptionIdentifier = subIdResult.Success;
               }
               else
               {
                  packet.HasMultipleSubscriptionIdentifiers = true;
               }
               break;
            case PropertyIdentifier.UserProperty:
               break;
            default:
               return new StringError($"Invalid property identifier. ({enumerator.Current.Identifier})");
         }
      }

      var consumedSoFar = rawPacket.Reader.Consumed - initialConsumed;
      var payloadLength = rawPacket.BodyLength - consumedSoFar;

      switch (payloadLength)
      {
         case < 0:
            return new StringError("Malformed packet: variable header length exceeds body length.");
         case > 0 when rawPacket.Reader.Remaining < payloadLength:
            return new StringError("Could not read payload.");
         case > 0:
            packet.Payload = rawPacket.Reader.Sequence.Slice(rawPacket.Reader.Position, payloadLength);
            rawPacket.Reader.Advance(payloadLength);
            break;
         default:
            packet.Payload = ReadOnlySequence<byte>.Empty;
            break;
      }

      var validateResult = PublishPacketVersion5Validator.Validate(ref packet);
      if (validateResult.Failed) return validateResult.Error;

      return PacketDispatchResult.Success;
   }
}
