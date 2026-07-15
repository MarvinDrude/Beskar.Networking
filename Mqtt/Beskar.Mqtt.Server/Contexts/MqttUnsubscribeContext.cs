using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttUnsubscribeContext
{
   public required MqttSession Session { get; init; }

   public required string TopicFilter { get; init; }
}
