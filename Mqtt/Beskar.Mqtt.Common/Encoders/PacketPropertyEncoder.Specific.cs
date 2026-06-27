using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders;

public ref partial struct PacketPropertyEncoder
{
   public void WriteSubscriptionIdentifiersAvailable(bool set)
   {
      if (set) return; // default is true so ignore
      Write(PropertyIdentifier.SubscriptionIdentifierAvailable, false);
   }

   public void WriteWillDelayInterval(uint value)
   {
      if (value == 0) return;
      Write(PropertyIdentifier.WillDelayInterval, value);
   }

   public void WriteWildcardSubscriptionAvailable(bool set)
   {
      if (set) return; // default is true
      Write(PropertyIdentifier.WildcardSubscriptionAvailable, false);
   }

   public void WriteUserProperty(ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> value)
   {
      Writer.WriteByte((byte)PropertyIdentifier.UserProperty);
      Write(nameUtf8);
      Write(value);
   }

   public void WriteTopicAliasMaximum(ushort value)
   {
      if (value == 0) return;
      Write(PropertyIdentifier.TopicAliasMaximum, value);
   }

   public void WriteTopicAlias(ushort value)
   {
      if (value == 0) return;
      Write(PropertyIdentifier.TopicAlias, value);
   }

   public void WriteSubscriptionIdentifier(uint value)
   {
      WriteVariable(PropertyIdentifier.SubscriptionIdentifier, value);
   }

   public void WriteSharedSubscriptionAvailable(bool set)
   {
      if (set) return;
      Write(PropertyIdentifier.SharedSubscriptionAvailable, false);
   }

   public void WriteSessionExpiryInterval(uint value)
   {
      if (value == 0) return;
      Write(PropertyIdentifier.SessionExpiryInterval, value);
   }

   public void WriteServerReference(ReadOnlySpan<byte> valueUtf8)
   {
      Write(PropertyIdentifier.ServerReference, valueUtf8);
   }

   public void WriteServerKeepAlive(ushort value)
   {
      if (value == 0) return;
      Write(PropertyIdentifier.ServerKeepAlive, value);
   }

   public void WriteRetainAvailable(bool set)
   {
      if (set) return;
      Write(PropertyIdentifier.RetainAvailable, false);
   }

   public void WriteResponseTopic(ReadOnlySpan<byte> valueUtf8)
   {
      Write(PropertyIdentifier.ResponseTopic, valueUtf8);
   }

   public void WriteResponseInformation(ReadOnlySpan<byte> valueUtf8)
   {
      Write(PropertyIdentifier.ResponseInformation, valueUtf8);
   }

   public void WriteRequestResponseInformation(bool set)
   {
      if (!set) return;
      Write(PropertyIdentifier.RequestResponseInformation, true);
   }

   public void WriteRequestProblemInformation(bool set)
   {
      if (set) return;
      Write(PropertyIdentifier.RequestProblemInformation, false);
   }

   public void WriteReceiveMaximum(ushort value)
   {
      if (value == 0) return;
      Write(PropertyIdentifier.ReceiveMaximum, value);
   }

   public void WriteReasonString(ReadOnlySpan<byte> valueUtf8)
   {
      Write(PropertyIdentifier.ReasonString, valueUtf8);
   }

   public void WritePayloadFormatIndicator(PayloadFormat value)
   {
      if (value is PayloadFormat.Unspecified) return;
      Write(PropertyIdentifier.PayloadFormatIndicator, (byte)value);
   }

   public void WriteMessageExpiryInterval(uint value)
   {
      if (value == 0) return;
      Write(PropertyIdentifier.MessageExpiryInterval, value);
   }

   public void WriteMaximumQoS(QualityOfServiceType value)
   {
      if (value is QualityOfServiceType.ExactlyOnce) return;
      Write(PropertyIdentifier.MaximumQos, value == QualityOfServiceType.AtLeastOnce ? (byte)0x1 : (byte)0x0);
   }

   public void WriteMaximumPacketSize(uint value)
   {
      if (value == 0) return;
      Write(PropertyIdentifier.MaximumPacketSize, value);
   }

   public void WriteCorrelationData(ReadOnlySpan<byte> value)
   {
      Write(PropertyIdentifier.CorrelationData, value);
   }

   public void WriteContentType(ReadOnlySpan<byte> valueUtf8)
   {
      Write(PropertyIdentifier.ContentType, valueUtf8);
   }

   public void WriteAuthenticationData(ReadOnlySpan<byte> value)
   {
      Write(PropertyIdentifier.AuthenticationData, value);
   }

   public void WriteAuthenticationMethod(ReadOnlySpan<byte> valueUtf8)
   {
      Write(PropertyIdentifier.AuthenticationMethod, valueUtf8);
   }

   public void WriteAssignedClientIdentifier(ReadOnlySpan<byte> valueUtf8)
   {
      Write(PropertyIdentifier.AssignedClientIdentifier, valueUtf8);
   }
}
