using Beskar.Memory.Code.EnumGenerator.Attributes;

namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// MQTT v5.0 PUBCOMP Reason Codes.
/// </summary>
[FastEnum]
public enum PubCompReasonCode : byte
{
   /// <summary>
   /// Success (0x00). The packet identifier is released.
   /// </summary>
   Success = 0x00,

   /// <summary>
   /// Packet Identifier not found (0x92). The packet identifier is not known to the receiver.
   /// </summary>
   PacketIdentifierNotFound = 0x92
}
