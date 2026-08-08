using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

/// <summary>
/// Event context for intercepting an incoming PUBLISH message on the server before routing to subscribers or updating retained messages.
/// </summary>
public sealed class MqttPublishInterceptContext
{
   /// <summary>
   /// Gets the client that sent the PUBLISH packet.
   /// </summary>
   public required MqttServerClient Client { get; init; }

   /// <summary>
   /// Gets the MQTT session associated with the publisher client.
   /// </summary>
   public required MqttSession Session { get; init; }

   /// <summary>
   /// Gets the publish message containing topic, payload, QoS, retain, and properties.
   /// </summary>
   public required MqttPublishMessage PublishMessage { get; init; }

   /// <summary>
   /// Gets or sets whether the incoming publish message should be blocked (ignored).
   /// When set to true, the message will not be dispatched to subscribers or stored as a retained message.
   /// </summary>
   public bool IsBlocked { get; set; }

   /// <summary>
   /// Gets or sets the ReasonCode returned in PUBACK (QoS 1) or PUBREC (QoS 2) when the message is processed or blocked.
   /// Defaults to Success. Set to an error code (e.g., NotAuthorized, ImplementationSpecificError, QuotaExceeded) if desired.
   /// </summary>
   public byte ReasonCode { get; set; } = (byte)PubAckReasonCode.Success;

   /// <summary>
   /// Blocks (ignores) the incoming published message.
   /// </summary>
   /// <param name="reasonCode">Optional reason code to return in PUBACK/PUBREC (defaults to Success for silent drop).</param>
   public void Block(byte reasonCode = (byte)PubAckReasonCode.Success)
   {
      IsBlocked = true;
      ReasonCode = reasonCode;
   }
}
