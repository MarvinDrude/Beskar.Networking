namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when the MQTT server stops.
/// </summary>
public sealed class MqttServerStopContext
{
   /// <summary>
   /// Gets the MQTT server instance.
   /// </summary>
   public required MqttServer Server { get; init; }
}
