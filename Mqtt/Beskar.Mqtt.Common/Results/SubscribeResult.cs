namespace Beskar.Mqtt.Common.Results;

public sealed class SubscribeResult
{
   /// <summary>
   /// Packet Identifier of the SUBSCRIBE.
   /// </summary>
   public required ushort PacketIdentifier { get; init; }

   /// <summary>
   /// Reason string
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public required string ReasonString { get; init; }


}
