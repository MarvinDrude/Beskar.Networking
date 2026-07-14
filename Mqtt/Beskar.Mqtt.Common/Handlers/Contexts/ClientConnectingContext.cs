using Beskar.Mqtt.Common.Builders.Connecting;

namespace Beskar.Mqtt.Common.Handlers.Contexts;

/// <summary>
/// Event context for when the MQTT client is connecting.
/// (Before anything really happens)
/// </summary>
public sealed class ClientConnectingContext
{
   /// <summary>
   /// The options that are going to be used to connect to the MQTT server.
   /// </summary>
   public required ConnectOptions ConnectOptions { get; init; }
}
