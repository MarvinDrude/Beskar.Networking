using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when a client attempts to subscribe to a topic filter.
/// </summary>
public sealed class MqttSubscribeContext
{
   /// <summary>
   /// Gets the MQTT session of the client subscribing.
   /// </summary>
   public required MqttSession Session { get; init; }

   /// <summary>
   /// Gets the topic filter being subscribed to.
   /// </summary>
   public required string TopicFilter { get; init; }

   /// <summary>
   /// Gets the requested Quality of Service (QoS) level for the subscription.
   /// </summary>
   public required QualityOfServiceType QualityOfService { get; init; }

   /// <summary>
   /// Gets a value indicating whether messages published by this client should not be forwarded back to it on this subscription (No Local option).
   /// </summary>
   public required bool NoLocal { get; init; }

   /// <summary>
   /// Gets a value indicating whether messages forwarded on this subscription should keep their retain flag set as they were originally published.
   /// </summary>
   public required bool RetainAsPublished { get; init; }

   /// <summary>
   /// Gets the retain handling type defining how retained messages are sent when the subscription is established.
   /// </summary>
   public required RetainHandlingType RetainHandling { get; init; }
}
