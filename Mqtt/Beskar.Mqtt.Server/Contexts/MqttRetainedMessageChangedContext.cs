using System.Collections.Generic;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttRetainedMessageChangedContext
{
   public required string ClientId { get; init; }

   public MqttPublishMessage? ChangedRetainedMessage { get; init; }

   public required IReadOnlyList<MqttPublishMessage> StoredRetainedMessages { get; init; }
}
