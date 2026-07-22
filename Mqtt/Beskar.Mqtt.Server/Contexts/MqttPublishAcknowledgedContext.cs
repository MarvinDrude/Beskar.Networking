using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when a publish operation has been acknowledged by the receiver.
/// </summary>
public sealed class MqttPublishAcknowledgedContext
{
   /// <summary>
   /// Gets the MQTT session of the client that acknowledged the publish.
   /// </summary>
   public required MqttSession Session { get; init; }

   /// <summary>
   /// Gets the details of the pending publish message that was acknowledged.
   /// </summary>
   public required MqttPendingPublish PendingPublish { get; init; }
}
