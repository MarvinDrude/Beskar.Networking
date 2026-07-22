using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when a new client session has been created on the MQTT server.
/// </summary>
public sealed class MqttNewSessionContext
{
   /// <summary>
   /// Gets the newly created MQTT session.
   /// </summary>
   public required MqttSession Session { get; init; }
}
