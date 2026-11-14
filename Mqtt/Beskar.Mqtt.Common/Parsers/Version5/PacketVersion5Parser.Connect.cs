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
         var identifier = propEnumerator.Current.Identifier;
         switch (identifier)
         {
            case PropertyIdentifier.SessionExpiryInterval:
               if (!TryMarkProperty(ref foundFlags, 0x01)) return DuplicatePropertyError(identifier);
               packet.SessionExpiryInterval = propEnumerator.Current.AsSessionExpiryInterval();
               break;
            case PropertyIdentifier.RequestProblemInformation:
               if (!TryMarkProperty(ref foundFlags, 0x02)) return DuplicatePropertyError(identifier);
               packet.RequestProblemInfo = propEnumerator.Current.AsRequestProblemInfo();
               break;
            case PropertyIdentifier.RequestResponseInformation:
               if (!TryMarkProperty(ref foundFlags, 0x04)) return DuplicatePropertyError(identifier);
               packet.RequestResponseInfo = propEnumerator.Current.AsRequestResponseInfo();
               break;
            case PropertyIdentifier.MaximumPacketSize:
               if (!TryMarkProperty(ref foundFlags, 0x08)) return DuplicatePropertyError(identifier);
               packet.MaximumPacketSize = propEnumerator.Current.AsMaximumPacketSize();
               break;
            case PropertyIdentifier.AuthenticationMethod:
               if (!TryMarkProperty(ref foundFlags, 0x10)) return DuplicatePropertyError(identifier);
               packet.AuthenticationMethodUtf8Bytes = propEnumerator.Current.AsAuthenticationMethod();
               break;
            case PropertyIdentifier.AuthenticationData:
               if (!TryMarkProperty(ref foundFlags, 0x20)) return DuplicatePropertyError(identifier);
               packet.AuthenticationDataBytes = propEnumerator.Current.AsAuthenticationData();
               break;
            case PropertyIdentifier.ReceiveMaximum:
               if (!TryMarkProperty(ref foundFlags, 0x40)) return DuplicatePropertyError(identifier);
               packet.ReceiveMaximum = propEnumerator.Current.AsReceiveMaximum();
               break;
            case PropertyIdentifier.TopicAliasMaximum:
               if (!TryMarkProperty(ref foundFlags, 0x80)) return DuplicatePropertyError(identifier);
               packet.TopicAliasMaximum = propEnumerator.Current.AsTopicAliasMaximum();
               break;
            case PropertyIdentifier.UserProperty:
               break;
            default:
               return new StringError($"Invalid property identifier. ({identifier})");
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
            var identifier = enumerator.Current.Identifier;
            switch (identifier)
            {
               case PropertyIdentifier.PayloadFormatIndicator:
                  if (!TryMarkProperty(ref foundFlags, 0b000001)) return DuplicatePropertyError(identifier);
                  packet.WillPayloadFormatIndicator = enumerator.Current.AsPayloadFormat();
                  break;
               case PropertyIdentifier.MessageExpiryInterval:
                  if (!TryMarkProperty(ref foundFlags, 0b000010)) return DuplicatePropertyError(identifier);
                  packet.WillMessageExpiryInterval = enumerator.Current.AsMessageExpiryInterval();
                  break;
               case PropertyIdentifier.WillDelayInterval:
                  if (!TryMarkProperty(ref foundFlags, 0b000100)) return DuplicatePropertyError(identifier);
                  packet.WillDelayInterval = enumerator.Current.AsWillDelayInterval();
                  break;
               case PropertyIdentifier.ContentType:
                  if (!TryMarkProperty(ref foundFlags, 0b001000)) return DuplicatePropertyError(identifier);
                  packet.WillContentTypeUtf8Bytes = enumerator.Current.AsContentType();
                  break;
               case PropertyIdentifier.CorrelationData:
                  if (!TryMarkProperty(ref foundFlags, 0b010000)) return DuplicatePropertyError(identifier);
                  packet.WillCorrelationDataBytes = enumerator.Current.AsCorrelationData();
                  break;
               case PropertyIdentifier.ResponseTopic:
                  if (!TryMarkProperty(ref foundFlags, 0b100000)) return DuplicatePropertyError(identifier);
                  packet.WillResponseTopicUtf8Bytes = enumerator.Current.AsResponseTopic();
                  break;
               case PropertyIdentifier.UserProperty:
                  break;
               default:
                  return new StringError($"Invalid property identifier. ({identifier})");
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

   private static bool TryMarkProperty(ref byte foundFlags, byte bit)
   {
      if ((foundFlags & bit) != 0)
      {
         return false;
      }

      foundFlags |= bit;
      return true;
   }

   private static StringError DuplicatePropertyError(PropertyIdentifier identifier)
      => new($"Protocol Violation: Duplicate property identifier. ({identifier})");
}
