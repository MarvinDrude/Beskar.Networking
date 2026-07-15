using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttDeleteSessionContext
{
   public required MqttSession Session { get; init; }
}
