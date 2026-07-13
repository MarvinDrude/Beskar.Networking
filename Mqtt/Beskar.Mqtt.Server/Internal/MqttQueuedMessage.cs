using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Server.Internal;

public sealed class MqttQueuedMessage(
   MqttPublishMessage message,
   QualityOfServiceType qos,
   bool retainAsPublished,
   uint subscriptionIdentifier)
{
   public MqttPublishMessage Message { get; } = message;
   public QualityOfServiceType QualityOfService { get; } = qos;

   public bool RetainAsPublished { get; } = retainAsPublished;
   public uint SubscriptionIdentifier { get; } = subscriptionIdentifier;
}
