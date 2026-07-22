using Beskar.Mqtt.Client.States;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server.Enums;
using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for when a client disconnects from the MQTT server.
/// </summary>
public sealed class MqttDisconnectContext
{
   /// <summary>
   /// Gets the server client instance that disconnected.
   /// </summary>
   public required MqttServerClient ServerClient { get; init; }

   /// <summary>
   /// Gets the reason code specifying why the client disconnected.
   /// </summary>
   public required DisconnectReasonCode Reason { get; init; }

   /// <summary>
   /// Gets the kind of client disconnect (e.g. Clean, KeepAliveTimeout, ProtocolError, etc.).
   /// </summary>
   public required ClientDisconnectKind DisconnectKind { get; init; }

   /// <summary>
   /// Gets a value indicating whether the disconnect was caused by another client session taking over this Client ID.
   /// </summary>
   public required bool IsSessionTakenOver { get; init; }
}
