using System.Buffers;
using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Protocol.Models;

/// <summary>
/// Represents a received or parsed MQTT PUBLISH message on the heap.
/// </summary>
public sealed class MqttPublishMessage
{
   /// <summary>
   /// Gets a value indicating whether this is a duplicate delivery of an earlier PUBLISH packet.
   /// </summary>
   public bool Dup { get; }

   /// <summary>
   /// Gets the Quality of Service (QoS) level for the message.
   /// </summary>
   public QualityOfServiceType QualityOfService { get; }

   /// <summary>
   /// Gets a value indicating whether the message should be retained by the broker.
   /// </summary>
   public bool Retain { get; }

   /// <summary>
   /// Gets the topic name to which the message is published.
   /// </summary>
   public string Topic { get; }

   /// <summary>
   /// Gets the packet identifier, which is unique within the session for QoS > 0.
   /// </summary>
   public ushort PacketIdentifier { get; }

   /// <summary>
   /// Gets the Payload Format Indicator.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PayloadFormat PayloadFormat { get; }

   /// <summary>
   /// Gets the Message Expiry Interval in seconds.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public uint MessageExpiryInterval { get; }

   /// <summary>
   /// Gets the Topic Alias identifier.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ushort TopicAlias { get; }

   /// <summary>
   /// Gets the Response Topic.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public string? ResponseTopic { get; }

   /// <summary>
   /// Gets the Correlation Data.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ReadOnlyMemory<byte>? CorrelationData { get; }

   /// <summary>
   /// Gets the Content Type of the payload.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public string? ContentType { get; }

   /// <summary>
   /// Gets the application payload data.
   /// </summary>
   public ReadOnlyMemory<byte> Payload { get; }

   /// <summary>
   /// Gets the User Properties associated with the message.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public UserPropertyCollection UserProperties { get; }

   /// <summary>
   /// Gets the subscription identifiers associated with the message.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public IReadOnlyList<uint> SubscriptionIdentifiers { get; }

   /// <summary>
   /// Initializes a new instance of the <see cref="MqttPublishMessage"/> class from a <see cref="PublishPacket"/>.
   /// </summary>
   /// <param name="packet">The parsed publish packet.</param>
   public MqttPublishMessage(in PublishPacket packet)
   {
      Dup = packet.Dup;
      QualityOfService = packet.QualityOfService;
      Retain = packet.Retain;

      Topic = packet.TopicUtf8Bytes.GetUtf8String() ?? string.Empty;
      PacketIdentifier = packet.PacketIdentifier;

      PayloadFormat = packet.PayloadFormat;
      MessageExpiryInterval = packet.MessageExpiryInterval;
      TopicAlias = packet.TopicAlias;

      ResponseTopic = packet.ResponseTopicUtf8Bytes.GetUtf8String();
      CorrelationData = packet.CorrelationDataBytes.ToNullableMemory();

      ContentType = packet.ContentTypeUtf8Bytes.GetUtf8String();
      Payload = packet.Payload.ToArray();

      UserProperties = UserPropertyCollection.Create(packet.PropertiesBytes);

      var subscriptionIdentifiers = new List<uint>();
      var subIdEnumerator = packet.GetSubscriptionIdentifiers();

      while (subIdEnumerator.MoveNext())
      {
         subscriptionIdentifiers.Add(subIdEnumerator.Current);
      }

      SubscriptionIdentifiers = subscriptionIdentifiers;
   }
}
