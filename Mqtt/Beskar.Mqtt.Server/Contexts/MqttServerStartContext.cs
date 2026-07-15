namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttServerStartContext
{
   public required MqttServer Server { get; init; }
}
