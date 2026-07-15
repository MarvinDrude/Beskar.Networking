using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttSubscribeContext
{
   public required MqttSession Session { get; init; }

   public required string TopicFilter { get; init; }

   public required QualityOfServiceType QualityOfService { get; init; }

   public required bool NoLocal { get; init; }

   public required bool RetainAsPublished { get; init; }

   public required RetainHandlingType RetainHandling { get; init; }
}
