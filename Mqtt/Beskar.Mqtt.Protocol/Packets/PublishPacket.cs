using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public ref struct PublishPacket
{
   public bool Dup;
   public QualityOfServiceType QualityOfService;
   public bool Retain;

   public ReadOnlySequence<byte> TopicUtf8Bytes;
   public ushort PacketIdentifier;

   public PayloadFormat PayloadFormat;
   public uint MessageExpiryInterval;
   public ushort TopicAlias;

   public ReadOnlySequence<byte> ResponseTopicUtf8Bytes;
   public ReadOnlySequence<byte> CorrelationDataBytes;

   public ReadOnlySequence<byte> ContentTypeUtf8Bytes;
   public ReadOnlySequence<byte> Payload;

   public ReadOnlySequence<byte> PropertiesBytes;
   public MqttPropertyEnumerator GetProperties() => new(PropertiesBytes);

   public uint SubscriptionIdentifier;
   public bool HasMultipleSubscriptionIdentifiers;

   public SubscriptionIdentifierEnumerator GetSubscriptionIdentifiers() => new(PropertiesBytes);

   public override string ToString()
   {
      return "PUBLISH";
   }

   internal string DebuggerDisplay => ToString();
}
