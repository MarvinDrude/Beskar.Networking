using Beskar.Memory.Code.EnumGenerator.Attributes;

namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// Connect Reason Code values (sent in CONNACK).
/// 3.2.2.2 Connect Reason Code
/// </summary>
[FastEnum]
public enum ConnectReasonCode : byte
{
   /// <summary>
   /// Success (0x00).
   /// <para>The Connection is accepted.</para>
   /// </summary>
   Success = 0x00,

   /// <summary>
   /// Unspecified error (0x80).
   /// <para>The Server does not wish to reveal the reason for the failure, or none of the other Reason Codes apply.</para>
   /// </summary>
   UnspecifiedError = 0x80,

   /// <summary>
   /// Malformed Packet (0x81).
   /// <para>Data within the CONNECT packet could not be correctly parsed.</para>
   /// </summary>
   MalformedPacket = 0x81,

   /// <summary>
   /// Protocol Error (0x82).
   /// <para>Data in the CONNECT packet does not conform to this specification.</para>
   /// </summary>
   ProtocolError = 0x82,

   /// <summary>
   /// Implementation specific error (0x83).
   /// <para>The CONNECT is valid but is not accepted by this Server.</para>
   /// </summary>
   ImplementationSpecificError = 0x83,

   /// <summary>
   /// Unsupported Protocol Version (0x84).
   /// <para>The Server does not support the version of the MQTT protocol requested by the Client.</para>
   /// </summary>
   UnsupportedProtocolVersion = 0x84,

   /// <summary>
   /// Client Identifier not valid (0x85).
   /// <para>The Client Identifier is a valid string but is not allowed by the Server.</para>
   /// </summary>
   ClientIdentifierNotValid = 0x85,

   /// <summary>
   /// Bad User Name or Password (0x86).
   /// <para>The Server does not accept the User Name or Password specified by the Client.</para>
   /// </summary>
   BadUserNameOrPassword = 0x86,

   /// <summary>
   /// Not authorized (0x87).
   /// <para>The Client is not authorized to connect.</para>
   /// </summary>
   NotAuthorized = 0x87,

   /// <summary>
   /// Server unavailable (0x88).
   /// <para>The MQTT Server is not available.</para>
   /// </summary>
   ServerUnavailable = 0x88,

   /// <summary>
   /// Server busy (0x89).
   /// <para>The Server is busy. Try again later.</para>
   /// </summary>
   ServerBusy = 0x89,

   /// <summary>
   /// Banned (0x8A).
   /// <para>This Client has been banned by administrative action. Contact the server administrator.</para>
   /// </summary>
   Banned = 0x8A,

   /// <summary>
   /// Bad authentication method (0x8C).
   /// <para>The authentication method is not supported or does not match the authentication method currently in use.</para>
   /// </summary>
   BadAuthenticationMethod = 0x8C,

   /// <summary>
   /// Topic Name invalid (0x90).
   /// <para>The Will Topic Name is not malformed, but is not accepted by this Server.</para>
   /// </summary>
   TopicNameInvalid = 0x90,

   /// <summary>
   /// Packet too large (0x95).
   /// <para>The CONNECT packet exceeded the maximum permissible size.</para>
   /// </summary>
   PacketTooLarge = 0x95,

   /// <summary>
   /// Quota exceeded (0x97).
   /// <para>An implementation or administrative imposed limit has been exceeded.</para>
   /// </summary>
   QuotaExceeded = 0x97,

   /// <summary>
   /// Payload format invalid (0x99).
   /// <para>The Will Payload does not match the specified Payload Format Indicator.</para>
   /// </summary>
   PayloadFormatInvalid = 0x99,

   /// <summary>
   /// Retain not supported (0x9A).
   /// <para>The Server does not support retained messages, and Will Retain was set to 1.</para>
   /// </summary>
   RetainNotSupported = 0x9A,

   /// <summary>
   /// QoS not supported (0x9B).
   /// <para>The Server does not support the QoS set in Will QoS.</para>
   /// </summary>
   QosNotSupported = 0x9B,

   /// <summary>
   /// Use another server (0x9C).
   /// <para>The Client should temporarily use another server.</para>
   /// </summary>
   UseAnotherServer = 0x9C,

   /// <summary>
   /// Server moved (0x9D).
   /// <para>The Client should permanently use another server.</para>
   /// </summary>
   ServerMoved = 0x9D,

   /// <summary>
   /// Connection rate exceeded (0x9F).
   /// <para>The connection rate limit has been exceeded.</para>
   /// </summary>
   ConnectionRateExceeded = 0x9F
}
