namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// MQTT v5.0 Subscription option for Retain Handling.
/// </summary>
public enum RetainHandlingType : byte
{
   /// <summary>
   /// Send retained messages at the time of the subscribe (0).
   /// </summary>
   SendAtSubscription = 0,

   /// <summary>
   /// Send retained messages at subscribe only if the subscription does not currently exist (1).
   /// </summary>
   SendOnNewSubscriptionOnly = 1,

   /// <summary>
   /// Do not send retained messages at the time of the subscribe (2).
   /// </summary>
   DoNotSend = 2
}
