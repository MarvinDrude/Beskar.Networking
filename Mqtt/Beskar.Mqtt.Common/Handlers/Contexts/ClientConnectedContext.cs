using Beskar.Mqtt.Protocol.Results;

namespace Beskar.Mqtt.Common.Handlers.Contexts;

/// <summary>
/// Event context for when the MQTT client has successfully connected.
/// </summary>
public sealed class ClientConnectedContext
{
   /// <summary>
   /// Gets the connection handshake result returned by the server.
   /// </summary>
   public required ClientConnectResult ConnectResult { get; init; }
}
