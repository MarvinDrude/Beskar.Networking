using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Results;

public readonly struct PublishResult
{
   /// <summary>
   /// The packet identifier which was used in this publish.
   /// </summary>
   public ushort? PacketIdentifier { get; init; }

   /// <summary>
   /// The Reason code. (used for MQTT 5.0)
   /// </summary>
   public PubAckReasonCode ReasonCode { get; init; }

   /// <summary>
   /// The Reason string. (used for MQTT 5.0)
   /// </summary>
   public string? ReasonString { get; init; }

   /// <summary>
   /// User properties returned by the server.
   /// </summary>
   public UserPropertyCollection UserProperties { get; init; }
}
