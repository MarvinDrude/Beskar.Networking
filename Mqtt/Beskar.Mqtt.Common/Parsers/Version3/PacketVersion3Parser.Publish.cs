using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Extensions;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Mqtt.Protocol.Validators.Version3;

namespace Beskar.Mqtt.Common.Parsers.Version3;

public readonly ref partial struct PacketVersion3Parser
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

      var consumedSoFar = rawPacket.Reader.Consumed - initialConsumed;
      var payloadLength = rawPacket.BodyLength - consumedSoFar;

      if (payloadLength > 0)
      {
         if (rawPacket.Reader.Remaining < payloadLength)
         {
            return new StringError("Could not read payload.");
         }

         packet.Payload = rawPacket.Reader.Sequence.Slice(rawPacket.Reader.Position, payloadLength);
         rawPacket.Reader.Advance(payloadLength);
      }
      else
      {
         packet.Payload = ReadOnlySequence<byte>.Empty;
      }

      var validateResult = PublishPacketVersion3Validator.Validate(ref packet);
      if (validateResult.Failed) return validateResult.Error;

      return PacketDispatchResult.Success;
   }
}
