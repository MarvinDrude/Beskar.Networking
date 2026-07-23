using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when a publish message was sent but no matching subscriber was found.
/// </summary>
public sealed class MqttNoSubscriberMessageContext
{
   /// <summary>
   /// Gets the MQTT session of the client that published the message.
   /// </summary>
   public required MqttSession Session { get; init; }

   /// <summary>
   /// Gets the published message that has no subscribers.
   /// </summary>
   public required MqttPublishMessage PublishMessage { get; init; }
}
