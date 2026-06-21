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
   /// Parses the CONNECT packet variable header and payload (after the protocol name and version have been parsed).
   /// </summary>
   private static Result<PacketDispatchResult, StringError> TryParseConnectPacket(
      ref RawPacket rawPacket,
      ref ConnectPacket packet)
   {
      // Protocol name and version are already parsed by PacketParser.
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
      var passwordFlagRaw = (connectFlags & 0x40) > 0;
      var usernameFlag = (connectFlags & 0x80) > 0;

      if (passwordFlagRaw && !usernameFlag)
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

      if (!rawPacket.Reader.TryReadProperties(out packet.PropertiesBytes))
      {
         return new StringError("Could not read properties.");
      }

      var propEnumerator = packet.GetProperties();
      byte foundFlags = 0;

      while (propEnumerator.MoveNext())
      {
         switch (propEnumerator.Current.Identifier)
         {
            case PropertyIdentifier.SessionExpiryInterval:
               packet.SessionExpiryInterval = propEnumerator.Current.AsSessionExpiryInterval();
               foundFlags |= 0x01;
               break;
            case PropertyIdentifier.RequestProblemInformation:
               packet.RequestProblemInfo = propEnumerator.Current.AsRequestProblemInfo();
               foundFlags |= 0x02;
               break;
            case PropertyIdentifier.RequestResponseInformation:
               packet.RequestResponseInfo = propEnumerator.Current.AsRequestResponseInfo();
               foundFlags |= 0x04;
               break;
            case PropertyIdentifier.MaximumPacketSize:
               packet.MaximumPacketSize = propEnumerator.Current.AsMaximumPacketSize();
               foundFlags |= 0x08;
               break;
            case PropertyIdentifier.AuthenticationMethod:
               packet.AuthenticationMethodUtf8Bytes = propEnumerator.Current.AsAuthenticationMethod();
               foundFlags |= 0x10;
               break;
            case PropertyIdentifier.AuthenticationData:
               packet.AuthenticationDataBytes = propEnumerator.Current.AsAuthenticationData();
               foundFlags |= 0x20;
               break;
            case PropertyIdentifier.ReceiveMaximum:
               packet.ReceiveMaximum = propEnumerator.Current.AsReceiveMaximum();
               foundFlags |= 0x40;
               break;
            case PropertyIdentifier.TopicAliasMaximum:
               packet.TopicAliasMaximum = propEnumerator.Current.AsTopicAliasMaximum();
               foundFlags |= 0x80;
               break;
            case PropertyIdentifier.UserProperty:
               break;
            default:
               return new StringError($"Invalid property identifier. ({propEnumerator.Current.Identifier})");
         }

         if (foundFlags == 0xFF)
         {
            break;
         }
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

         if (!rawPacket.Reader.TryReadProperties(out packet.WillPropertiesBytes))
         {
            return new StringError("Could not read will properties.");
         }

         var enumerator = packet.GetWillProperties();
         foundFlags = 0;

         while (enumerator.MoveNext())
         {
            switch (enumerator.Current.Identifier)
            {
               case PropertyIdentifier.PayloadFormatIndicator:
                  packet.WillPayloadFormatIndicator = enumerator.Current.AsPayloadFormat();
                  foundFlags |= 0b000001;
                  break;
               case PropertyIdentifier.MessageExpiryInterval:
                  packet.WillMessageExpiryInterval = enumerator.Current.AsMessageExpiryInterval();
                  foundFlags |= 0b000010;
                  break;
               case PropertyIdentifier.WillDelayInterval:
                  packet.WillDelayInterval = enumerator.Current.AsWillDelayInterval();
                  foundFlags |= 0b000100;
                  break;
               case PropertyIdentifier.ContentType:
                  packet.WillContentTypeUtf8Bytes = enumerator.Current.AsContentType();
                  foundFlags |= 0b001000;
                  break;
               case PropertyIdentifier.CorrelationData:
                  packet.WillCorrelationDataBytes = enumerator.Current.AsCorrelationData();
                  foundFlags |= 0b010000;
                  break;
               case PropertyIdentifier.ResponseTopic:
                  packet.WillResponseTopicUtf8Bytes = enumerator.Current.AsResponseTopic();
                  foundFlags |= 0b100000;
                  break;
               case PropertyIdentifier.UserProperty:
                  break;
               default:
                  return new StringError($"Invalid property identifier. ({enumerator.Current.Identifier})");
            }

            if (foundFlags == 0b111111)
            {
               break;
            }
         }

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

      if (passwordFlagRaw)
      {
         if (!rawPacket.Reader.TryReadRawBytes(out packet.PasswordBytes))
         {
            return new StringError("Could not read password.");
         }
      }

      var validateResult = ConnectPacketVersion5Validator.Validate(ref packet);
      if (validateResult.Failed) return validateResult.Error;

      return PacketDispatchResult.Success;
   }
}
