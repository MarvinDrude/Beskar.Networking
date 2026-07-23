using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Server.Internal;

/// <summary>
/// Represents a publish message that is in-flight or unacknowledged by the client.
/// </summary>
public sealed class MqttPendingPublish
{
   /// <summary>
   /// Gets the packet identifier assigned to this publish operation.
   /// </summary>
   public required ushort PacketIdentifier { get; init; }

   /// <summary>
   /// Gets the underlying MQTT publish message.
   /// </summary>
   public required MqttPublishMessage Message { get; init; }

   /// <summary>
   /// Gets the Quality of Service (QoS) level used for this publish operation.
   /// </summary>
   public required QualityOfServiceType QualityOfService { get; init; }

   /// <summary>
   /// Gets a value indicating whether the message should be delivered with the retain flag set as it was originally published.
   /// </summary>
   public required bool RetainAsPublished { get; init; }

   /// <summary>
   /// Gets the identifier of the subscription that matched this message, if any.
   /// </summary>
   public required uint SubscriptionIdentifier { get; init; }
}
