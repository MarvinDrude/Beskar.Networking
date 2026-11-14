using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteConnAck(in ConnAckPacket packet)
   {
      var existingMask = CollectPresentProperties(packet.PropertiesBytes);
      var propsDataLen = CalculateConnAckPropertiesDataLength(in packet, existingMask);
      var remainingLength = 2 + PacketEncoder.GetVariableByteIntegerLength(propsDataLen) + propsDataLen;
      var length = PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.ConnAck, 0, remainingLength);

         var connAckFlags = (byte)(packet.IsSessionPresent ? 1 : 0);
         writer.WriteByte(connAckFlags);
         writer.WriteByte((byte)packet.ReasonCode);

         PacketEncoder.WriteVariableByteInteger(ref writer, (uint)propsDataLen);
         if (propsDataLen > 0)
         {
            var propEncoder = writer.AsConnAckPropertyEncoder();
            try
            {
               if (packet.SessionExpiryInterval != 0
                   && !HasProperty(existingMask, PropertyIdentifier.SessionExpiryInterval))
               {
                  propEncoder.WriteSessionExpiryInterval(packet.SessionExpiryInterval);
               }

               if (packet.ReceiveMaximum != 0
                   && !HasProperty(existingMask, PropertyIdentifier.ReceiveMaximum))
               {
                  propEncoder.WriteReceiveMaximum(packet.ReceiveMaximum);
               }

               if (packet.MaximumPacketSize != 0
                   && !HasProperty(existingMask, PropertyIdentifier.MaximumPacketSize))
               {
                  propEncoder.WriteMaximumPacketSize(packet.MaximumPacketSize);
               }

               if (packet.TopicAliasMaximum != 0
                   && !HasProperty(existingMask, PropertyIdentifier.TopicAliasMaximum))
               {
                  propEncoder.WriteTopicAliasMaximum(packet.TopicAliasMaximum);
               }

               if (packet.ServerKeepAlive != 0
                   && !HasProperty(existingMask, PropertyIdentifier.ServerKeepAlive))
               {
                  propEncoder.WriteServerKeepAlive(packet.ServerKeepAlive);
               }

               if (!packet.ReasonStringUtf8Bytes.IsEmpty
                   && !HasProperty(existingMask, PropertyIdentifier.ReasonString))
               {
                  propEncoder.Encoder.Write(PropertyIdentifier.ReasonString, packet.ReasonStringUtf8Bytes);
               }

               if (!packet.ServerReferenceUtf8Bytes.IsEmpty
                   && !HasProperty(existingMask, PropertyIdentifier.ServerReference))
               {
                  propEncoder.Encoder.Write(PropertyIdentifier.ServerReference, packet.ServerReferenceUtf8Bytes);
               }

               if (!packet.ResponseInfoUtf8Bytes.IsEmpty
                   && !HasProperty(existingMask, PropertyIdentifier.ResponseInformation))
               {
                  propEncoder.Encoder.Write(PropertyIdentifier.ResponseInformation, packet.ResponseInfoUtf8Bytes);
               }

               if (!packet.AssignedClientIdentifierUtf8Bytes.IsEmpty
                   && !HasProperty(existingMask, PropertyIdentifier.AssignedClientIdentifier))
               {
                  propEncoder.Encoder.Write(PropertyIdentifier.AssignedClientIdentifier, packet.AssignedClientIdentifierUtf8Bytes);
               }

               if (!packet.AuthenticationMethodUtf8Bytes.IsEmpty
                   && !HasProperty(existingMask, PropertyIdentifier.AuthenticationMethod))
               {
                  propEncoder.Encoder.Write(PropertyIdentifier.AuthenticationMethod, packet.AuthenticationMethodUtf8Bytes);
               }

               if (!packet.AuthenticationDataBytes.IsEmpty
                   && !HasProperty(existingMask, PropertyIdentifier.AuthenticationData))
               {
                  propEncoder.Encoder.Write(PropertyIdentifier.AuthenticationData, packet.AuthenticationDataBytes);
               }

               if (!packet.IsRetainAvailable
                   && !HasProperty(existingMask, PropertyIdentifier.RetainAvailable))
               {
                  propEncoder.WriteRetainAvailable(false);
               }

               if (!packet.IsSharedSubscriptionAvailable
                   && !HasProperty(existingMask, PropertyIdentifier.SharedSubscriptionAvailable))
               {
                  propEncoder.WriteSharedSubscriptionAvailable(false);
               }

               if (!packet.IsSubscriptionIdentifierAvailable
                   && !HasProperty(existingMask, PropertyIdentifier.SubscriptionIdentifierAvailable))
               {
                  propEncoder.WriteSubscriptionIdentifiersAvailable(false);
               }

               if (!packet.IsWildcardSubscriptionAvailable
                   && !HasProperty(existingMask, PropertyIdentifier.WildcardSubscriptionAvailable))
               {
                  propEncoder.WriteWildcardSubscriptionAvailable(false);
               }

               if (packet.MaximumQualityOfService != QualityOfServiceType.ExactlyOnce
                   && !HasProperty(existingMask, PropertyIdentifier.MaximumQos))
               {
                  propEncoder.WriteMaximumQoS(packet.MaximumQualityOfService);
               }
            }
            finally
            {
               writer = propEncoder.Encoder.Writer;
            }

            PacketEncoder.WriteRawSequence(ref writer, packet.PropertiesBytes);
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static int CalculateConnAckPropertiesDataLength(in ConnAckPacket packet, ulong existingMask)
   {
      var len = (int)packet.PropertiesBytes.Length;

      if (packet.SessionExpiryInterval != 0 && !HasProperty(existingMask, PropertyIdentifier.SessionExpiryInterval))
      {
         len += 5;
      }

      if (packet.ReceiveMaximum != 0 && !HasProperty(existingMask, PropertyIdentifier.ReceiveMaximum))
      {
         len += 3;
      }

      if (packet.MaximumPacketSize != 0 && !HasProperty(existingMask, PropertyIdentifier.MaximumPacketSize))
      {
         len += 5;
      }

      if (packet.TopicAliasMaximum != 0 && !HasProperty(existingMask, PropertyIdentifier.TopicAliasMaximum))
      {
         len += 3;
      }

      if (packet.ServerKeepAlive != 0 && !HasProperty(existingMask, PropertyIdentifier.ServerKeepAlive))
      {
         len += 3;
      }

      if (!packet.ReasonStringUtf8Bytes.IsEmpty && !HasProperty(existingMask, PropertyIdentifier.ReasonString))
      {
         len += 3 + (int)packet.ReasonStringUtf8Bytes.Length;
      }

      if (!packet.ServerReferenceUtf8Bytes.IsEmpty && !HasProperty(existingMask, PropertyIdentifier.ServerReference))
      {
         len += 3 + (int)packet.ServerReferenceUtf8Bytes.Length;
      }

      if (!packet.ResponseInfoUtf8Bytes.IsEmpty && !HasProperty(existingMask, PropertyIdentifier.ResponseInformation))
      {
         len += 3 + (int)packet.ResponseInfoUtf8Bytes.Length;
      }

      if (!packet.AssignedClientIdentifierUtf8Bytes.IsEmpty
          && !HasProperty(existingMask, PropertyIdentifier.AssignedClientIdentifier))
      {
         len += 3 + (int)packet.AssignedClientIdentifierUtf8Bytes.Length;
      }

      if (!packet.AuthenticationMethodUtf8Bytes.IsEmpty
          && !HasProperty(existingMask, PropertyIdentifier.AuthenticationMethod))
      {
         len += 3 + (int)packet.AuthenticationMethodUtf8Bytes.Length;
      }

      if (!packet.AuthenticationDataBytes.IsEmpty
          && !HasProperty(existingMask, PropertyIdentifier.AuthenticationData))
      {
         len += 3 + (int)packet.AuthenticationDataBytes.Length;
      }

      if (!packet.IsRetainAvailable && !HasProperty(existingMask, PropertyIdentifier.RetainAvailable))
      {
         len += 2;
      }

      if (!packet.IsSharedSubscriptionAvailable && !HasProperty(existingMask, PropertyIdentifier.SharedSubscriptionAvailable))
      {
         len += 2;
      }

      if (!packet.IsSubscriptionIdentifierAvailable && !HasProperty(existingMask, PropertyIdentifier.SubscriptionIdentifierAvailable))
      {
         len += 2;
      }

      if (!packet.IsWildcardSubscriptionAvailable && !HasProperty(existingMask, PropertyIdentifier.WildcardSubscriptionAvailable))
      {
         len += 2;
      }

      if (packet.MaximumQualityOfService != QualityOfServiceType.ExactlyOnce
          && !HasProperty(existingMask, PropertyIdentifier.MaximumQos))
      {
         len += 2;
      }

      return len;
   }
}
