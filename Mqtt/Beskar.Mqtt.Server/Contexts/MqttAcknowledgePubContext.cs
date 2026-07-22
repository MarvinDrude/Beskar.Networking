using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when a publish packet has been acknowledged by the server.
/// </summary>
public sealed class MqttAcknowledgePubContext
{
   /// <summary>
   /// Gets the MQTT session associated with the publish acknowledgment.
   /// </summary>
   public required MqttSession Session { get; init; }

   /// <summary>
   /// Gets the published message that was acknowledged.
   /// </summary>
   public required MqttPublishMessage PublishMessage { get; init; }
}
