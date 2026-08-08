using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Common.Models;

/// <summary>
/// Represents the response context received from a subscriber in a Request-Response interaction.
/// </summary>
public sealed class MqttResponseContext
{
   /// <summary>
   /// Gets the complete received MQTT publish message from the subscriber.
   /// </summary>
   public required MqttPublishMessage Message { get; init; }

   /// <summary>
   /// Gets the payload data of the response message.
   /// </summary>
   public ReadOnlyMemory<byte> Payload => Message.Payload;

   /// <summary>
   /// Gets the correlation identifier string matched for this request, if present.
   /// </summary>
   public string? CorrelationId { get; init; }

   /// <summary>
   /// Gets the total round-trip time elapsed from transmitting the request to receiving the response.
   /// </summary>
   public TimeSpan Elapsed { get; init; }
}
