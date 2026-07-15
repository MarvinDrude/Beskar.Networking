using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Server.Internal;

public sealed class MqttPendingPublish
{
   public required ushort PacketIdentifier { get; init; }

   public required MqttPublishMessage Message { get; init; }

   public required QualityOfServiceType QualityOfService { get; init; }

   public required bool RetainAsPublished { get; init; }

   public required uint SubscriptionIdentifier { get; init; }
}
