namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when the MQTT server starts up.
/// </summary>
public sealed class MqttServerStartContext
{
   /// <summary>
   /// Gets the MQTT server instance.
   /// </summary>
   public required MqttServer Server { get; init; }
}
