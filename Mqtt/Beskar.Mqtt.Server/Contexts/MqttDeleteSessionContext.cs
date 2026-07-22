using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when a server session is deleted or cleaned up.
/// </summary>
public sealed class MqttDeleteSessionContext
{
   /// <summary>
   /// Gets the MQTT session that is being deleted.
   /// </summary>
   public required MqttSession Session { get; init; }
}
