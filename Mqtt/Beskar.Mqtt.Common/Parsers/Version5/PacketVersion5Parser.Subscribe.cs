using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Mqtt.Protocol.Validators.Version5;

namespace Beskar.Mqtt.Common.Parsers.Version5;

public readonly ref partial struct PacketVersion5Parser
{
   /// <summary>
   /// Parses the SUBSCRIBE packet variable header and payload.
   /// </summary>
   private static Result<PacketDispatchResult, StringError> TryParseSubscribePacket(
      ref RawPacket rawPacket,
      ref SubscribePacket packet)
   {
      if ((rawPacket.FixedHeader & 0x0F) != 0x02)
      {
         return new StringError("Protocol Violation: Reserved bits in SUBSCRIBE fixed header must be 0010.");
      }

      var initialConsumed = rawPacket.Reader.Consumed;

      if (!rawPacket.Reader.TryReadUInt16BigEndian(out packet.PacketIdentifier))
      {
         return new StringError("Could not read packet identifier.");
      }

      if (!rawPacket.Reader.TryReadProperties(out packet.PropertiesBytes))
      {
         return new StringError("Could not read properties.");
      }



      var consumedSoFar = rawPacket.Reader.Consumed - initialConsumed;
      var payloadLength = rawPacket.BodyLength - consumedSoFar;

      if (payloadLength < 0)
      {
         return new StringError("Malformed packet: variable header length exceeds body length.");
      }

      if (payloadLength > 0)
      {
         if (rawPacket.Reader.Remaining < payloadLength)
         {
            return new StringError("Could not read subscriptions payload.");
         }

         packet.FiltersBytes = rawPacket.Reader.Sequence.Slice(rawPacket.Reader.Position, payloadLength);
         rawPacket.Reader.Advance(payloadLength);
      }
      else
      {
         packet.FiltersBytes = ReadOnlySequence<byte>.Empty;
      }

      var validateResult = SubscribePacketVersion5Validator.Validate(ref packet);
      if (validateResult.Failed) return validateResult.Error;

      return PacketDispatchResult.Success;
   }
}
