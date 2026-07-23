using System.Collections.Generic;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when a retained message is added, updated, or removed on the server.
/// </summary>
public sealed class MqttRetainedMessageChangedContext
{
   /// <summary>
   /// Gets the identifier of the client that caused the retained message change.
   /// </summary>
   public required string ClientId { get; init; }

   /// <summary>
   /// Gets the retained message that was changed, or <c>null</c> if it was deleted.
   /// </summary>
   public MqttPublishMessage? ChangedRetainedMessage { get; init; }

   /// <summary>
   /// Gets the complete list of all currently stored retained messages on the server.
   /// </summary>
   public required IReadOnlyList<MqttPublishMessage> StoredRetainedMessages { get; init; }
}
