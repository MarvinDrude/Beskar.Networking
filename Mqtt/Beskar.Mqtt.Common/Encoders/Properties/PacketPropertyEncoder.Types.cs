using System;
using System.Runtime.InteropServices;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders.Properties;

public static class PacketPropertyEncoderExtensions
{
   extension(ByteWriter writer)
   {
      public ConnectPropertyEncoder AsConnectPropertyEncoder() => new(new PacketPropertyEncoder(writer));
      public ConnAckPropertyEncoder AsConnAckPropertyEncoder() => new(new PacketPropertyEncoder(writer));
      public PublishPropertyEncoder AsPublishPropertyEncoder() => new(new PacketPropertyEncoder(writer));
      public PubAckPropertyEncoder AsPubAckPropertyEncoder() => new(new PacketPropertyEncoder(writer));
      public SubscribePropertyEncoder AsSubscribePropertyEncoder() => new(new PacketPropertyEncoder(writer));
      public SubAckPropertyEncoder AsSubAckPropertyEncoder() => new(new PacketPropertyEncoder(writer));
      public UnsubscribePropertyEncoder AsUnsubscribePropertyEncoder() => new(new PacketPropertyEncoder(writer));
      public UnsubAckPropertyEncoder AsUnsubAckPropertyEncoder() => new(new PacketPropertyEncoder(writer));
      public DisconnectPropertyEncoder AsDisconnectPropertyEncoder() => new(new PacketPropertyEncoder(writer));
      public AuthPropertyEncoder AsAuthPropertyEncoder() => new(new PacketPropertyEncoder(writer));
      public WillPropertyEncoder AsWillPropertyEncoder() => new(new PacketPropertyEncoder(writer));
   }
}

[StructLayout(LayoutKind.Auto)]
public ref struct ConnectPropertyEncoder(PacketPropertyEncoder encoder)
{
   public PacketPropertyEncoder Encoder = encoder;

   public void WriteSessionExpiryInterval(uint value) => Encoder.WriteSessionExpiryInterval(value);
   public void WriteReceiveMaximum(ushort value) => Encoder.WriteReceiveMaximum(value);
   public void WriteMaximumPacketSize(uint value) => Encoder.WriteMaximumPacketSize(value);
   public void WriteTopicAliasMaximum(ushort value) => Encoder.WriteTopicAliasMaximum(value);
   public void WriteRequestResponseInformation(bool set) => Encoder.WriteRequestResponseInformation(set);
   public void WriteRequestProblemInformation(bool set) => Encoder.WriteRequestProblemInformation(set);
   public void WriteUserProperty(ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> value) => Encoder.WriteUserProperty(nameUtf8, value);
   public void WriteAuthenticationMethod(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteAuthenticationMethod(valueUtf8);
   public void WriteAuthenticationData(ReadOnlySpan<byte> value) => Encoder.WriteAuthenticationData(value);
}

[StructLayout(LayoutKind.Auto)]
public ref struct ConnAckPropertyEncoder(PacketPropertyEncoder encoder)
{
   public PacketPropertyEncoder Encoder = encoder;

   public void WriteSessionExpiryInterval(uint value) => Encoder.WriteSessionExpiryInterval(value);
   public void WriteReceiveMaximum(ushort value) => Encoder.WriteReceiveMaximum(value);
   public void WriteMaximumQoS(QualityOfServiceType value) => Encoder.WriteMaximumQoS(value);
   public void WriteRetainAvailable(bool set) => Encoder.WriteRetainAvailable(set);
   public void WriteMaximumPacketSize(uint value) => Encoder.WriteMaximumPacketSize(value);
   public void WriteAssignedClientIdentifier(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteAssignedClientIdentifier(valueUtf8);
   public void WriteTopicAliasMaximum(ushort value) => Encoder.WriteTopicAliasMaximum(value);
   public void WriteReasonString(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteReasonString(valueUtf8);
   public void WriteUserProperty(ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> value) => Encoder.WriteUserProperty(nameUtf8, value);
   public void WriteWildcardSubscriptionAvailable(bool set) => Encoder.WriteWildcardSubscriptionAvailable(set);
   public void WriteSubscriptionIdentifiersAvailable(bool set) => Encoder.WriteSubscriptionIdentifiersAvailable(set);
   public void WriteSharedSubscriptionAvailable(bool set) => Encoder.WriteSharedSubscriptionAvailable(set);
   public void WriteServerKeepAlive(ushort value) => Encoder.WriteServerKeepAlive(value);
   public void WriteResponseInformation(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteResponseInformation(valueUtf8);
   public void WriteServerReference(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteServerReference(valueUtf8);
   public void WriteAuthenticationMethod(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteAuthenticationMethod(valueUtf8);
   public void WriteAuthenticationData(ReadOnlySpan<byte> value) => Encoder.WriteAuthenticationData(value);
}

[StructLayout(LayoutKind.Auto)]
public ref struct PublishPropertyEncoder(PacketPropertyEncoder encoder)
{
   public PacketPropertyEncoder Encoder = encoder;

   public void WritePayloadFormatIndicator(PayloadFormat value) => Encoder.WritePayloadFormatIndicator(value);
   public void WriteMessageExpiryInterval(uint value) => Encoder.WriteMessageExpiryInterval(value);
   public void WriteContentType(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteContentType(valueUtf8);
   public void WriteResponseTopic(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteResponseTopic(valueUtf8);
   public void WriteCorrelationData(ReadOnlySpan<byte> value) => Encoder.WriteCorrelationData(value);
   public void WriteSubscriptionIdentifier(uint value) => Encoder.WriteSubscriptionIdentifier(value);
   public void WriteTopicAlias(ushort value) => Encoder.WriteTopicAlias(value);
   public void WriteUserProperty(ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> value) => Encoder.WriteUserProperty(nameUtf8, value);
}

[StructLayout(LayoutKind.Auto)]
public ref struct PubAckPropertyEncoder(PacketPropertyEncoder encoder)
{
   public PacketPropertyEncoder Encoder = encoder;

   public void WriteReasonString(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteReasonString(valueUtf8);
   public void WriteUserProperty(ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> value) => Encoder.WriteUserProperty(nameUtf8, value);
}

[StructLayout(LayoutKind.Auto)]
public ref struct SubscribePropertyEncoder(PacketPropertyEncoder encoder)
{
   public PacketPropertyEncoder Encoder = encoder;

   public void WriteSubscriptionIdentifier(uint value) => Encoder.WriteSubscriptionIdentifier(value);
   public void WriteUserProperty(ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> value) => Encoder.WriteUserProperty(nameUtf8, value);
}

[StructLayout(LayoutKind.Auto)]
public ref struct SubAckPropertyEncoder(PacketPropertyEncoder encoder)
{
   public PacketPropertyEncoder Encoder = encoder;

   public void WriteReasonString(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteReasonString(valueUtf8);
   public void WriteUserProperty(ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> value) => Encoder.WriteUserProperty(nameUtf8, value);
}

[StructLayout(LayoutKind.Auto)]
public ref struct UnsubscribePropertyEncoder(PacketPropertyEncoder encoder)
{
   public PacketPropertyEncoder Encoder = encoder;

   public void WriteUserProperty(ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> value) => Encoder.WriteUserProperty(nameUtf8, value);
}

[StructLayout(LayoutKind.Auto)]
public ref struct UnsubAckPropertyEncoder(PacketPropertyEncoder encoder)
{
   public PacketPropertyEncoder Encoder = encoder;

   public void WriteReasonString(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteReasonString(valueUtf8);
   public void WriteUserProperty(ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> value) => Encoder.WriteUserProperty(nameUtf8, value);
}

[StructLayout(LayoutKind.Auto)]
public ref struct DisconnectPropertyEncoder(PacketPropertyEncoder encoder)
{
   public PacketPropertyEncoder Encoder = encoder;

   public void WriteSessionExpiryInterval(uint value) => Encoder.WriteSessionExpiryInterval(value);
   public void WriteReasonString(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteReasonString(valueUtf8);
   public void WriteUserProperty(ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> value) => Encoder.WriteUserProperty(nameUtf8, value);
   public void WriteServerReference(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteServerReference(valueUtf8);
}

[StructLayout(LayoutKind.Auto)]
public ref struct AuthPropertyEncoder(PacketPropertyEncoder encoder)
{
   public PacketPropertyEncoder Encoder = encoder;

   public void WriteAuthenticationMethod(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteAuthenticationMethod(valueUtf8);
   public void WriteAuthenticationData(ReadOnlySpan<byte> value) => Encoder.WriteAuthenticationData(value);
   public void WriteReasonString(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteReasonString(valueUtf8);
   public void WriteUserProperty(ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> value) => Encoder.WriteUserProperty(nameUtf8, value);
}

[StructLayout(LayoutKind.Auto)]
public ref struct WillPropertyEncoder(PacketPropertyEncoder encoder)
{
   public PacketPropertyEncoder Encoder = encoder;

   public void WriteWillDelayInterval(uint value) => Encoder.WriteWillDelayInterval(value);
   public void WritePayloadFormatIndicator(PayloadFormat value) => Encoder.WritePayloadFormatIndicator(value);
   public void WriteMessageExpiryInterval(uint value) => Encoder.WriteMessageExpiryInterval(value);
   public void WriteContentType(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteContentType(valueUtf8);
   public void WriteResponseTopic(ReadOnlySpan<byte> valueUtf8) => Encoder.WriteResponseTopic(valueUtf8);
   public void WriteCorrelationData(ReadOnlySpan<byte> value) => Encoder.WriteCorrelationData(value);
   public void WriteUserProperty(ReadOnlySpan<byte> nameUtf8, ReadOnlySpan<byte> value) => Encoder.WriteUserProperty(nameUtf8, value);
}
