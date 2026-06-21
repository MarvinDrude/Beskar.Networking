using Beskar.Memory.Code.EnumGenerator.Attributes;

namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// MQTT v5.0 PUBACK Reason Codes.
/// </summary>
[FastEnum]
public enum PubAckReasonCode : byte
{
   /// <summary>
   /// Success (0x00). The message is accepted.
   /// </summary>
   Success = 0x00,

   /// <summary>
   /// No matching subscribers (0x10). The message is accepted but there are no matching subscribers.
   /// </summary>
   NoMatchingSubscribers = 0x10,

   /// <summary>
   /// Unspecified error (0x80). The receiver does not wish to reveal the reason for failure, or none of the other Reason Codes apply.
   /// </summary>
   UnspecifiedError = 0x80,

   /// <summary>
   /// Implementation specific error (0x83). The packet is valid but cannot be processed by this implementation.
   /// </summary>
   ImplementationSpecificError = 0x83,

   /// <summary>
   /// Not authorized (0x87). The Client or Server is not authorized to publish to this topic.
   /// </summary>
   NotAuthorized = 0x87,

   /// <summary>
   /// Topic Name invalid (0x90). The Topic Name is correctly formed, but is not accepted by this Client or Server.
   /// </summary>
   TopicNameInvalid = 0x90,

   /// <summary>
   /// Packet Identifier in use (0x91). The Packet Identifier is already in use.
   /// </summary>
   PacketIdentifierInUse = 0x91,

   /// <summary>
   /// Quota exceeded (0x97). An implementation or administrative limit has been exceeded.
   /// </summary>
   QuotaExceeded = 0x97,

   /// <summary>
   /// Payload format invalid (0x99). The payload format does not match the one specified by the Payload Format Indicator.
   /// </summary>
   PayloadFormatInvalid = 0x99
}
