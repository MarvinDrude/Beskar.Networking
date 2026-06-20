namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// MQTT v5.0 SUBACK Reason Codes.
/// </summary>
public enum SubscribeReasonCode : byte
{
   /// <summary>
   /// Granted QoS 0 (0x00). The subscription is accepted and the maximum QoS is QoS 0.
   /// </summary>
   GrantedQos0 = 0x00,

   /// <summary>
   /// Granted QoS 1 (0x01). The subscription is accepted and the maximum QoS is QoS 1.
   /// </summary>
   GrantedQos1 = 0x01,

   /// <summary>
   /// Granted QoS 2 (0x02). The subscription is accepted and the maximum QoS is QoS 2.
   /// </summary>
   GrantedQos2 = 0x02,

   /// <summary>
   /// Unspecified error (0x80). The subscription is not accepted and the Server does not wish to reveal the reason.
   /// </summary>
   UnspecifiedError = 0x80,

   /// <summary>
   /// Implementation specific error (0x83). The subscription is valid but cannot be processed by this implementation.
   /// </summary>
   ImplementationSpecificError = 0x83,

   /// <summary>
   /// Not authorized (0x87). The client is not authorized to subscribe to this Topic Filter.
   /// </summary>
   NotAuthorized = 0x87,

   /// <summary>
   /// Topic Filter invalid (0x8F). The Topic Filter is correctly formed but is not accepted by this Server.
   /// </summary>
   TopicFilterInvalid = 0x8F,

   /// <summary>
   /// Packet Identifier in use (0x91). The Packet Identifier is already in use.
   /// </summary>
   PacketIdentifierInUse = 0x91,

   /// <summary>
   /// Quota exceeded (0x97). An implementation or administrative limit has been exceeded.
   /// </summary>
   QuotaExceeded = 0x97,

   /// <summary>
   /// Shared Subscriptions not supported (0x9E). The Server does not support Shared Subscriptions for this Client.
   /// </summary>
   SharedSubscriptionsNotSupported = 0x9E,

   /// <summary>
   /// Subscription Identifiers not supported (0xA1). The Server does not support Subscription Identifiers.
   /// </summary>
   SubscriptionIdentifiersNotSupported = 0xA1,

   /// <summary>
   /// Wildcard Subscriptions not supported (0xA2). The Server does not support Wildcard Subscriptions.
   /// </summary>
   WildcardSubscriptionsNotSupported = 0xA2
}
