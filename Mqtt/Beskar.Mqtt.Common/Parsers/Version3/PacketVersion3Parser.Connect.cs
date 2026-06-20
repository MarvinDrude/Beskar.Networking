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
   /// Parses the CONNECT packet variable header and payload (after the protocol name and version have been parsed).
   /// </summary>
   private static Result<PacketDispatchResult, StringError> TryParseConnectPacket(
      ref RawPacket rawPacket,
      ref ConnectPacket packet)
   {
      // protocol name and version, try private flag is already parsed at this point
      if (!rawPacket.Reader.TryRead(out var connectFlags))
      {
         return new StringError("Could not read connect flags.");
      }

      if ((connectFlags & 0x1) > 0)
      {
         return new StringError("First bit is not 0 set.");
      }

      packet.IsCleanSession = (connectFlags & 0x2) > 0;

      var willFlag = (connectFlags & 0x4) > 0;
      var willQoS = (connectFlags & 0x18) >> 3;
      var willRetain = (connectFlags & 0x20) > 0;
      var passwordFlag = (connectFlags & 0x40) > 0;
      var usernameFlag = (connectFlags & 0x80) > 0;

      if (passwordFlag && !usernameFlag)
      {
         return new StringError("Protocol Violation: Password Flag is set to 1, but User Name Flag is set to 0.");
      }

      if (!willFlag)
      {
         if (willQoS != 0 || willRetain)
         {
            return new StringError(
               "Protocol Violation: Will Flag is set to 0, but Will QoS and/or Will Retain are not set to 0.");
         }
      }
      else
      {
         if (willQoS == 3)
         {
            return new StringError("Protocol Violation: Will QoS cannot be 3.");
         }
      }

      if (!rawPacket.Reader.TryReadUInt16BigEndian(out packet.KeepAliveInterval))
      {
         return new StringError("Could not read keep alive interval.");
      }

      if (!rawPacket.Reader.TryReadRawString(out packet.ClientIdUtf8Bytes))
      {
         return new StringError("Could not read raw client id.");
      }

      if (willFlag)
      {
         packet.HasWill = true;
         packet.WillRetain = willRetain;
         packet.WillQualityOfService = (QualityOfServiceType)willQoS;

         if (!rawPacket.Reader.TryReadRawString(out packet.WillTopicUtf8Bytes))
         {
            return new StringError("Could not read will topic.");
         }

         if (!rawPacket.Reader.TryReadRawBytes(out packet.WillMessageBytes))
         {
            return new StringError("Could not read will message.");
         }
      }

      if (usernameFlag)
      {
         if (!rawPacket.Reader.TryReadRawString(out packet.UsernameUtf8Bytes))
         {
            return new StringError("Could not read username.");
         }
      }

      if (passwordFlag)
      {
         if (!rawPacket.Reader.TryReadRawBytes(out packet.PasswordBytes))
         {
            return new StringError("Could not read password.");
         }
      }

      var validateResult = ConnectPacketVersion3Validator.Validate(ref packet);
      if (validateResult.Failed) return validateResult.Error;

      return PacketDispatchResult.Success;
   }
}
