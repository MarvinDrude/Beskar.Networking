namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttRetainedMessagesClearedContext
{
   public required MqttServer Server { get; init; }
}
