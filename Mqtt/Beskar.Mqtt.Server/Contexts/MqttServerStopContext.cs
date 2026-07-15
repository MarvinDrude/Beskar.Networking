namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttServerStopContext
{
   public required MqttServer Server { get; init; }
}
