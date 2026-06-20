namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// MQTT v5.0 PUBREL Reason Codes.
/// </summary>
public enum PubRelReasonCode : byte
{
   /// <summary>
   /// Success (0x00). The packet identifier is found and the message has been released.
   /// </summary>
   Success = 0x00,

   /// <summary>
   /// Packet Identifier not found (0x92). The packet identifier is not known to the receiver.
   /// </summary>
   PacketIdentifierNotFound = 0x92
}
