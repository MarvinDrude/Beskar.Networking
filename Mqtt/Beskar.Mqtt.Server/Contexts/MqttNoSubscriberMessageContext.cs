using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttNoSubscriberMessageContext
{
   public required MqttSession Session { get; init; }

   public required MqttPublishMessage PublishMessage { get; init; }
}
