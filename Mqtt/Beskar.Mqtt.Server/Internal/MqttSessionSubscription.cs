using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Server.Internal;

public sealed class MqttSessionSubscription
{
   public QualityOfServiceType QualityOfService { get; set; }

   public bool NoLocal { get; set; }
   public bool RetainAsPublished { get; set; }

   public RetainHandlingType RetainHandling { get; set; }
   public uint SubscriptionIdentifier { get; set; }
}
