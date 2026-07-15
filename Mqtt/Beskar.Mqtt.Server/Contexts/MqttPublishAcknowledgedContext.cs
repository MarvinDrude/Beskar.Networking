using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttPublishAcknowledgedContext
{
   public required MqttSession Session { get; init; }

   public required MqttPendingPublish PendingPublish { get; init; }
}
