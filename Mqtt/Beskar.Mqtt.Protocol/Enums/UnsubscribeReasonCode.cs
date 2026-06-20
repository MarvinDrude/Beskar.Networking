namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// MQTT v5.0 UNSUBACK Reason Codes.
/// </summary>
public enum UnsubscribeReasonCode : byte
{
   /// <summary>
   /// Success (0x00). The subscription is deleted.
   /// </summary>
   Success = 0x00,

   /// <summary>
   /// No subscription existed (0x11). No subscription existed for the Topic Filter specified in the UNSUBSCRIBE packet.
   /// </summary>
   NoSubscriptionExisted = 0x11,

   /// <summary>
   /// Unspecified error (0x80). The unsubscription is not accepted and the Server does not wish to reveal the reason.
   /// </summary>
   UnspecifiedError = 0x80,

   /// <summary>
   /// Implementation specific error (0x83). The unsubscription is valid but cannot be processed by this implementation.
   /// </summary>
   ImplementationSpecificError = 0x83,

   /// <summary>
   /// Not authorized (0x87). The Client is not authorized to unsubscribe.
   /// </summary>
   NotAuthorized = 0x87,

   /// <summary>
   /// Topic Filter invalid (0x8F). The Topic Filter is correctly formed but is not accepted by this Server.
   /// </summary>
   TopicFilterInvalid = 0x8F,

   /// <summary>
   /// Packet Identifier in use (0x91). The Packet Identifier is already in use.
   /// </summary>
   PacketIdentifierInUse = 0x91
}
