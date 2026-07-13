using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Server.Internal;

/// <summary>
/// Details of an active subscription.
/// </summary>
public sealed class MqttSubscription(
   MqttSession session,
   byte[] topicFilter,
   QualityOfServiceType qualityOfService,
   bool noLocal,
   bool retainAsPublished,
   RetainHandlingType retainHandling,
   uint subscriptionIdentifier)
{
   public MqttSession Session { get; } = session;

   public byte[] TopicFilter { get; } = topicFilter;
   public QualityOfServiceType QualityOfService { get; set; } = qualityOfService;

   public bool NoLocal { get; set; } = noLocal;
   public bool RetainAsPublished { get; set; } = retainAsPublished;

   public RetainHandlingType RetainHandling { get; set; } = retainHandling;
   public uint SubscriptionIdentifier { get; set; } = subscriptionIdentifier;
}
