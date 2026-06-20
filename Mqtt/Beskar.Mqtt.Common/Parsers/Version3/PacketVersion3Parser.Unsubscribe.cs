using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Mqtt.Protocol.Validators.Version3;

namespace Beskar.Mqtt.Common.Parsers.Version3;

public readonly ref partial struct PacketVersion3Parser
{
   /// <summary>
   /// Parses the UNSUBSCRIBE packet variable header and payload.
   /// </summary>
   private static Result<PacketDispatchResult, StringError> TryParseUnsubscribePacket(
      ref RawPacket rawPacket,
      ref UnsubscribePacket packet)
   {
      if ((rawPacket.FixedHeader & 0x0F) != 0x02)
      {
         return new StringError("Protocol Violation: Reserved bits in UNSUBSCRIBE fixed header must be 0010.");
      }

      var initialConsumed = rawPacket.Reader.Consumed;

      if (!rawPacket.Reader.TryReadUInt16BigEndian(out packet.PacketIdentifier))
      {
         return new StringError("Could not read packet identifier.");
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
            return new StringError("Could not read unsubscribe payload.");
         }

         packet.FiltersBytes = rawPacket.Reader.Sequence.Slice(rawPacket.Reader.Position, payloadLength);
         rawPacket.Reader.Advance(payloadLength);
      }
      else
      {
         packet.FiltersBytes = ReadOnlySequence<byte>.Empty;
      }

      var validateResult = UnsubscribePacketVersion3Validator.Validate(ref packet);
      if (validateResult.Failed) return validateResult.Error;

      return PacketDispatchResult.Success;
   }
}
