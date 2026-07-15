using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttRetainedMessageChangedContext
{
   public required string ClientId { get; init; }

   public MqttPublishMessage? Message { get; init; }
}
