using System.Buffers;
using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Protocol.Models;

public sealed class MqttPublishMessage(in PublishPacket packet)
{
   public bool Dup { get; } = packet.Dup;
   public QualityOfServiceType QualityOfService { get; } = packet.QualityOfService;
   public bool Retain { get; } = packet.Retain;

   public string Topic { get; } = packet.TopicUtf8Bytes.GetUtf8String() ?? string.Empty;
   public ushort PacketIdentifier { get; } = packet.PacketIdentifier;

   public PayloadFormat PayloadFormat { get; } = packet.PayloadFormat;
   public uint MessageExpiryInterval { get; } = packet.MessageExpiryInterval;
   public ushort TopicAlias { get; } = packet.TopicAlias;

   public string? ResponseTopic { get; } = packet.ResponseTopicUtf8Bytes.GetUtf8String();
   public ReadOnlyMemory<byte>? CorrelationData { get; } = packet.CorrelationDataBytes.ToNullableMemory();

   public string? ContentType { get; } = packet.ContentTypeUtf8Bytes.GetUtf8String();
   public ReadOnlyMemory<byte> Payload { get; } = packet.Payload.ToArray();

   public UserPropertyCollection UserProperties { get; } = UserPropertyCollection.Create(packet.PropertiesBytes);

   public uint SubscriptionIdentifier { get; } = packet.SubscriptionIdentifier;
   public bool HasMultipleSubscriptionIdentifiers { get; } = packet.HasMultipleSubscriptionIdentifiers;
}
