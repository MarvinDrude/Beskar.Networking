using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttLoadingRetainedMessagesContext
{
   public required MqttServer Server { get; init; }

   public List<MqttPublishMessage> LoadedRetainedMessages { get; set; } = [];
}
