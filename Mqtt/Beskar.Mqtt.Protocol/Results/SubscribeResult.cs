using System.Collections.Generic;
using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Protocol.Results;

public sealed class SubscribeResult
{
   /// <summary>
   /// Packet Identifier of the SUBSCRIBE.
   /// </summary>
   public required ushort PacketIdentifier { get; init; }

   /// <summary>
   /// Reason string
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public required string? ReasonString { get; init; }

   /// <summary>
   /// The result of each topic subscription containing the topic filter and its corresponding reason code.
   /// </summary>
   public required IReadOnlyList<MqttTopicSubscriptionResult> Subscriptions { get; init; }

   /// <summary>
   /// User properties returned by the server.
   /// </summary>
   public required UserPropertyCollection UserProperties { get; init; }
}

public readonly struct MqttTopicSubscriptionResult
{
   /// <summary>
   /// The topic filter that was subscribed to.
   /// </summary>
   public required MqttTopicFilter TopicFilter { get; init; }

   /// <summary>
   /// The reason code returned by the broker for this subscription.
   /// </summary>
   public required SubscribeReasonCode ReasonCode { get; init; }
}
