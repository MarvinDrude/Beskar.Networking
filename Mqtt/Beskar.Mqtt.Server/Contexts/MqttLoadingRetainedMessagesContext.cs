using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for loading retained messages into the server memory during startup.
/// </summary>
public sealed class MqttLoadingRetainedMessagesContext
{
   /// <summary>
   /// Gets the MQTT server instance.
   /// </summary>
   public required MqttServer Server { get; init; }

   /// <summary>
   /// Gets or sets the list of loaded retained messages.
   /// </summary>
   public List<MqttPublishMessage> LoadedRetainedMessages { get; set; } = [];
}
