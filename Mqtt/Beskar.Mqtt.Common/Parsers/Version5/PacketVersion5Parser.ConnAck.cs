using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Parsers.Version5;

public readonly ref partial struct PacketVersion5Parser
{
   /// <summary>
   /// Parses the CONNACK packet variable header.
   /// </summary>
   private Result<PacketDispatchResult, StringError> TryParseConnAckPacket(
      ref RawPacket rawPacket,
      ref ConnAckPacket packet)
   {
      if ((rawPacket.FixedHeader & 0x0F) != 0)
      {
         return new StringError("Protocol Violation: Reserved bits in CONNACK fixed header must be 0.");
      }

      if (rawPacket.BodyLength < 2)
      {
         return new StringError("Protocol Violation: CONNACK packet body length must be at least 2.");
      }

      if (!rawPacket.Reader.TryRead(out var connAckFlags))
      {
         return new StringError("Could not read connect acknowledge flags.");
      }

      if ((connAckFlags & 0xFE) != 0)
      {
         return new StringError("Protocol Violation: Reserved bits in connect acknowledge flags must be 0 in MQTT v5.0.");
      }
      packet.IsSessionPresent = (connAckFlags & 0x01) > 0;

      if (!rawPacket.Reader.TryRead(out var reasonCodeByte))
      {
         return new StringError("Could not read reason code.");
      }

      packet.IsRetainAvailable = true;
      packet.IsSharedSubscriptionAvailable = true;
      packet.IsSubscriptionIdentifierAvailable = true;
      packet.IsWildcardSubscriptionAvailable = true;

      packet.MaximumQualityOfService = QualityOfServiceType.ExactlyOnce;

      packet.ReasonCode = (ConnectReasonCode)reasonCodeByte;
      packet.ReturnCode = packet.ReasonCode.ToReturnCode();

      if (rawPacket.BodyLength > 2)
      {
         if (!rawPacket.Reader.TryReadProperties(out packet.PropertiesBytes))
         {
            return new StringError("Could not read properties.");
         }

         var enumerator = packet.GetProperties();
         ushort foundFlags = 0;

         while (enumerator.MoveNext())
         {
            switch (enumerator.Current.Identifier)
            {
               case PropertyIdentifier.AuthenticationMethod:
                  packet.AuthenticationMethodUtf8Bytes = enumerator.Current.AsAuthenticationMethod();
                  foundFlags |= 0x0001;
                  break;
               case PropertyIdentifier.AuthenticationData:
                  packet.AuthenticationDataBytes = enumerator.Current.AsAuthenticationData();
                  foundFlags |= 0x0002;
                  break;
               case PropertyIdentifier.SessionExpiryInterval:
                  packet.SessionExpiryInterval = enumerator.Current.AsSessionExpiryInterval();
                  foundFlags |= 0x0004;
                  break;
               case PropertyIdentifier.RetainAvailable:
                  packet.IsRetainAvailable = enumerator.Current.AsRetainAvailable();
                  foundFlags |= 0x0008;
                  break;
               case PropertyIdentifier.ServerReference:
                  packet.ServerReferenceUtf8Bytes = enumerator.Current.AsServerReference();
                  foundFlags |= 0x0010;
                  break;
               case PropertyIdentifier.ResponseInformation:
                  packet.ResponseInfoUtf8Bytes = enumerator.Current.AsResponseInfo();
                  foundFlags |= 0x0020;
                  break;
               case PropertyIdentifier.SharedSubscriptionAvailable:
                  packet.IsSharedSubscriptionAvailable = enumerator.Current.AsSharedSubscriptionAvailable();
                  foundFlags |= 0x0040;
                  break;
               case PropertyIdentifier.SubscriptionIdentifierAvailable:
                  packet.IsSubscriptionIdentifierAvailable = enumerator.Current.AsSubscriptionIdentifierAvailable();
                  foundFlags |= 0x0080;
                  break;
               case PropertyIdentifier.WildcardSubscriptionAvailable:
                  packet.IsWildcardSubscriptionAvailable = enumerator.Current.AsWildcardSubscriptionAvailable();
                  foundFlags |= 0x0100;
                  break;
               case PropertyIdentifier.ServerKeepAlive:
                  packet.ServerKeepAlive = enumerator.Current.AsServerKeepAlive();
                  foundFlags |= 0x0200;
                  break;
               case PropertyIdentifier.AssignedClientIdentifier:
                  packet.AssignedClientIdentifierUtf8Bytes = enumerator.Current.AsAssignedClientIdentifier();
                  foundFlags |= 0x0400;
                  break;
               case PropertyIdentifier.ReasonString:
                  packet.ReasonStringUtf8Bytes = enumerator.Current.AsReasonString();
                  foundFlags |= 0x0800;
                  break;
               case PropertyIdentifier.TopicAliasMaximum:
                  packet.TopicAliasMaximum = enumerator.Current.AsTopicAliasMaximum();
                  foundFlags |= 0x1000;
                  break;
               case PropertyIdentifier.MaximumPacketSize:
                  packet.MaximumPacketSize = enumerator.Current.AsMaximumPacketSize();
                  foundFlags |= 0x2000;
                  break;
               case PropertyIdentifier.ReceiveMaximum:
                  packet.ReceiveMaximum = enumerator.Current.AsReceiveMaximum();
                  foundFlags |= 0x4000;
                  break;
               case PropertyIdentifier.MaximumQos:
                  var maxRes = enumerator.Current.AsMaximumQualityOfService();
                  if (maxRes.Failed) return maxRes.Error;

                  packet.MaximumQualityOfService = maxRes.Success;
                  foundFlags |= 0x8000;
                  break;
               case PropertyIdentifier.UserProperty:
                  break;
               default:
                  return new StringError($"Invalid property identifier. ({enumerator.Current.Identifier})");
            }

            if (foundFlags == 0xFFFF)
            {
               break;
            }
         }
      }
      else
      {
         packet.PropertiesBytes = System.Buffers.ReadOnlySequence<byte>.Empty;
      }

      return PacketDispatchResult.Success;
   }
}
