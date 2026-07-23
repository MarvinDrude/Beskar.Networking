using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when a client has successfully completed the CONNECT handshake.
/// </summary>
public sealed class MqttConnectContext
{
   /// <summary>
   /// Gets the server client instance associated with the connection.
   /// </summary>
   public required MqttServerClient Client { get; init; }
}
