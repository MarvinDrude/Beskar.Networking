namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// 3.14.2.1 Disconnect Reason Code
/// Disconnect Reason Code values.
/// </summary>
public enum DisconnectReasonCode : byte
{
   /// <summary>
   /// Normal disconnection (0x00).
   /// <para>Sent by: Client or Server.</para>
   /// <para>Close the connection normally. Do not send the Will Message.</para>
   /// </summary>
   NormalDisconnection = 0x00,

   /// <summary>
   /// Disconnect with Will Message (0x04).
   /// <para>Sent by: Client.</para>
   /// <para>The Client wishes to disconnect but requires that the Server also publishes its Will Message.</para>
   /// </summary>
   DisconnectWithWillMessage = 0x04,

   /// <summary>
   /// Unspecified error (0x80).
   /// <para>Sent by: Client or Server.</para>
   /// <para>The Connection is closed but the sender either does not wish to reveal the reason, or none of the other Reason Codes apply.</para>
   /// </summary>
   UnspecifiedError = 0x80,

   /// <summary>
   /// Malformed Packet (0x81).
   /// <para>Sent by: Client or Server.</para>
   /// <para>The received packet does not conform to this specification.</para>
   /// </summary>
   MalformedPacket = 0x81,

   /// <summary>
   /// Protocol Error (0x82).
   /// <para>Sent by: Client or Server.</para>
   /// <para>An unexpected or out of order packet was received.</para>
   /// </summary>
   ProtocolError = 0x82,

   /// <summary>
   /// Implementation specific error (0x83).
   /// <para>Sent by: Client or Server.</para>
   /// <para>The packet received is valid but cannot be processed by this implementation.</para>
   /// </summary>
   ImplementationSpecificError = 0x83,

   /// <summary>
   /// Not authorized (0x87).
   /// <para>Sent by: Server.</para>
   /// <para>The request is not authorized.</para>
   /// </summary>
   NotAuthorized = 0x87,

   /// <summary>
   /// Server busy (0x89).
   /// <para>Sent by: Server.</para>
   /// <para>The Server is busy and cannot continue processing requests from this Client.</para>
   /// </summary>
   ServerBusy = 0x89,

   /// <summary>
   /// Server shutting down (0x8B).
   /// <para>Sent by: Server.</para>
   /// <para>The Server is shutting down.</para>
   /// </summary>
   ServerShuttingDown = 0x8B,

   /// <summary>
   /// Keep Alive timeout (0x8D).
   /// <para>Sent by: Server.</para>
   /// <para>The Connection is closed because no packet has been received for 1.5 times the Keepalive time.</para>
   /// </summary>
   KeepAliveTimeout = 0x8D,

   /// <summary>
   /// Session taken over (0x8E).
   /// <para>Sent by: Server.</para>
   /// <para>Another Connection using the same ClientID has connected causing this Connection to be closed.</para>
   /// </summary>
   SessionTakenOver = 0x8E,

   /// <summary>
   /// Topic Filter invalid (0x8F).
   /// <para>Sent by: Server.</para>
   /// <para>The Topic Filter is correctly formed, but is not accepted by this Sever.</para>
   /// </summary>
   TopicFilterInvalid = 0x8F,

   /// <summary>
   /// Topic Name invalid (0x90).
   /// <para>Sent by: Client or Server.</para>
   /// <para>The Topic Name is correctly formed, but is not accepted by this Client or Server.</para>
   /// </summary>
   TopicNameInvalid = 0x90,

   /// <summary>
   /// Receive Maximum exceeded (0x93).
   /// <para>Sent by: Client or Server.</para>
   /// <para>The Client or Server has received more than Receive Maximum publication for which it has not sent PUBACK or PUBCOMP.</para>
   /// </summary>
   ReceiveMaximumExceeded = 0x93,

   /// <summary>
   /// Topic Alias invalid (0x94).
   /// <para>Sent by: Client or Server.</para>
   /// <para>The Client or Server has received a PUBLISH packet containing a Topic Alias which is greater than the Maximum Topic Alias it sent in the CONNECT or CONNACK packet.</para>
   /// </summary>
   TopicAliasInvalid = 0x94,

   /// <summary>
   /// Packet too large (0x95).
   /// <para>Sent by: Client or Server.</para>
   /// <para>The packet size is greater than Maximum Packet Size for this Client or Server.</para>
   /// </summary>
   PacketTooLarge = 0x95,

   /// <summary>
   /// Message rate too high (0x96).
   /// <para>Sent by: Client or Server.</para>
   /// <para>The received data rate is too high.</para>
   /// </summary>
   MessageRateTooHigh = 0x96,

   /// <summary>
   /// Quota exceeded (0x97).
   /// <para>Sent by: Client or Server.</para>
   /// <para>An implementation or administrative imposed limit has been exceeded.</para>
   /// </summary>
   QuotaExceeded = 0x97,

   /// <summary>
   /// Administrative action (0x98).
   /// <para>Sent by: Client or Server.</para>
   /// <para>The Connection is closed due to an administrative action.</para>
   /// </summary>
   AdministrativeAction = 0x98,

   /// <summary>
   /// Payload format invalid (0x99).
   /// <para>Sent by: Client or Server.</para>
   /// <para>The payload format does not match the one specified by the Payload Format Indicator.</para>
   /// </summary>
   PayloadFormatInvalid = 0x99,

   /// <summary>
   /// Retain not supported (0x9A).
   /// <para>Sent by: Server.</para>
   /// <para>The Server has does not support retained messages.</para>
   /// </summary>
   RetainNotSupported = 0x9A,

   /// <summary>
   /// QoS not supported (0x9B).
   /// <para>Sent by: Server.</para>
   /// <para>The Client specified a QoS greater than the QoS specified in a Maximum QoS in the CONNACK.</para>
   /// </summary>
   QosNotSupported = 0x9B,

   /// <summary>
   /// Use another server (0x9C).
   /// <para>Sent by: Server.</para>
   /// <para>The Client should temporarily change its Server.</para>
   /// </summary>
   UseAnotherServer = 0x9C,

   /// <summary>
   /// Server moved (0x9D).
   /// <para>Sent by: Server.</para>
   /// <para>The Server is moved and the Client should permanently change its server location.</para>
   /// </summary>
   ServerMoved = 0x9D,

   /// <summary>
   /// Shared Subscriptions not supported (0x9E).
   /// <para>Sent by: Server.</para>
   /// <para>The Server does not support Shared Subscriptions.</para>
   /// </summary>
   SharedSubscriptionsNotSupported = 0x9E,

   /// <summary>
   /// Connection rate exceeded (0x9F).
   /// <para>Sent by: Server.</para>
   /// <para>This connection is closed because the connection rate is too high.</para>
   /// </summary>
   ConnectionRateExceeded = 0x9F,

   /// <summary>
   /// Maximum connect time (0xA0).
   /// <para>Sent by: Server.</para>
   /// <para>The maximum connection time authorized for this connection has been exceeded.</para>
   /// </summary>
   MaximumConnectTime = 0xA0,

   /// <summary>
   /// Subscription Identifiers not supported (0xA1).
   /// <para>Sent by: Server.</para>
   /// <para>The Server does not support Subscription Identifiers; the subscription is not accepted.</para>
   /// </summary>
   SubscriptionIdentifiersNotSupported = 0xA1,

   /// <summary>
   /// Wildcard Subscriptions not supported (0xA2).
   /// <para>Sent by: Server.</para>
   /// <para>The Server does not support Wildcard Subscriptions; the subscription is not accepted.</para>
   /// </summary>
   WildcardSubscriptionsNotSupported = 0xA2
}
