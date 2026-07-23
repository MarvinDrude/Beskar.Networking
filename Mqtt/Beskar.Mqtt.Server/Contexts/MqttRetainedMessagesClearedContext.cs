namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when all retained messages are cleared from the server.
/// </summary>
public sealed class MqttRetainedMessagesClearedContext
{
   /// <summary>
   /// Gets the MQTT server instance.
   /// </summary>
   public required MqttServer Server { get; init; }
}
