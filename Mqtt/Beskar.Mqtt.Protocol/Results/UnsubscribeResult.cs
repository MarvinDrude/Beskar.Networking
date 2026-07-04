using System.Collections.Generic;
using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Results;

public sealed class UnsubscribeResult
{
   /// <summary>
   /// Packet Identifier of the UNSUBSCRIBE.
   /// </summary>
   public required ushort PacketIdentifier { get; init; }

   /// <summary>
   /// Reason string
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public required string? ReasonString { get; init; }

   /// <summary>
   /// The result of each topic unsubscription containing the topic filter and its corresponding reason code.
   /// </summary>
   public required IReadOnlyList<MqttTopicUnsubscriptionResult> Unsubscriptions { get; init; }

   /// <summary>
   /// User properties returned by the server.
   /// </summary>
   public required UserPropertyCollection UserProperties { get; init; }
}

public readonly struct MqttTopicUnsubscriptionResult
{
   /// <summary>
   /// The topic filter that was unsubscribed from.
   /// </summary>
   public required string TopicFilter { get; init; }

   /// <summary>
   /// The reason code returned by the broker for this unsubscription.
   /// </summary>
   public required UnsubscribeReasonCode ReasonCode { get; init; }
}
