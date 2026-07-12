using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttConnectContext
{
   public required MqttServerClient Client { get; init; }
}
