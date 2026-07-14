using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Handlers.Contexts;

/// <summary>
/// Event context for when the MQTT client disconnects.
/// </summary>
public sealed class ClientDisconnectedContext
{
   /// <summary>
   /// User Properties if any sent.
   /// </summary>
   public UserPropertyCollection? UserProperties { get; init; }

   /// <summary>
   /// The disconnect Reason code if provided.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public DisconnectReasonCode ReasonCode { get; init; }

   /// <summary>
   /// The Reason string if provided.
   /// </summary>
   public string? ReasonString { get; init; }

   /// <summary>
   /// Whether the client was fully connected before disconnecting.
   /// </summary>
   public required bool BeforeConnected { get; init; }

   /// <summary>
   /// Any exception that occurred during the disconnect process.
   /// </summary>
   public Exception? Exception { get; init; }
}
