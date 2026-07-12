using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttNewSessionContext
{
   public required MqttSession Session { get; init; }
}
