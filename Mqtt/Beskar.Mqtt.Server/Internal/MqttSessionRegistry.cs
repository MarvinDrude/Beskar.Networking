namespace Beskar.Mqtt.Server.Internal;

public sealed class MqttSessionRegistry
{
   private readonly Dictionary<string, MqttSession> _sessions = new();
}
