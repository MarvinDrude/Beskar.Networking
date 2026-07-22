using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when a client unsubscribes from a topic filter.
/// </summary>
public sealed class MqttUnsubscribeContext
{
   /// <summary>
   /// Gets the MQTT session of the client unsubscribing.
   /// </summary>
   public required MqttSession Session { get; init; }

   /// <summary>
   /// Gets the topic filter being unsubscribed from.
   /// </summary>
   public required string TopicFilter { get; init; }
}
