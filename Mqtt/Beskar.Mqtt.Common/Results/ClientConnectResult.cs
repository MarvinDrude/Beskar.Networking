using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Results;

public sealed class ClientConnectResult
{
   /// <summary>
   /// Whether a session already exists on the server.
   /// </summary>
   public required bool IsSessionPresent { get; init; }

   /// <summary>
   /// The return code (used for MQTT 3.1.1).
   /// </summary>
   public required ConnectReturnCode ReturnCode { get; init; }

   /// <summary>
   /// The reason code (used for MQTT 5.0).
   /// </summary>
   public required ConnectReasonCode ReasonCode { get; init; }

   /// <summary>
   /// Reason string containing the human-readable reason for the result.
   /// </summary>
   public string? ReasonString { get; init; }

   /// <summary>
   /// Whether Retain is available.
   /// </summary>
   public bool? IsRetainAvailable { get; init; }

   /// <summary>
   /// Whether Shared Subscriptions are available.
   /// </summary>
   public bool? IsSharedSubscriptionAvailable { get; init; }

   /// <summary>
   /// Whether Subscription Identifiers are available.
   /// </summary>
   public bool? IsSubscriptionIdentifierAvailable { get; init; }

   /// <summary>
   /// Whether Wildcard Subscriptions are available.
   /// </summary>
   public bool? IsWildcardSubscriptionAvailable { get; init; }

   /// <summary>
   /// The maximum Quality of Service supported.
   /// </summary>
   public QualityOfServiceType? MaximumQualityOfService { get; init; }

   /// <summary>
   /// Session Expiry Interval.
   /// </summary>
   public uint? SessionExpiryInterval { get; init; }

   /// <summary>
   /// Server Keep Alive.
   /// </summary>
   public ushort? ServerKeepAlive { get; init; }

   /// <summary>
   /// Topic Alias Maximum.
   /// </summary>
   public ushort? TopicAliasMaximum { get; init; }

   /// <summary>
   /// Maximum Packet Size.
   /// </summary>
   public uint? MaximumPacketSize { get; init; }

   /// <summary>
   /// Receive Maximum.
   /// </summary>
   public ushort? ReceiveMaximum { get; init; }

   /// <summary>
   /// Authentication Method.
   /// </summary>
   public string? AuthenticationMethod { get; init; }

   /// <summary>
   /// Authentication Data.
   /// </summary>
   public ReadOnlyMemory<byte>? AuthenticationData { get; init; }

   /// <summary>
   /// Server Reference.
   /// </summary>
   public string? ServerReference { get; init; }

   /// <summary>
   /// Response Information.
   /// </summary>
   public string? ResponseInfo { get; init; }

   /// <summary>
   /// Assigned Client Identifier.
   /// </summary>
   public string? AssignedClientIdentifier { get; init; }

   /// <summary>
   /// User properties returned by the server.
   /// </summary>
   public required UserPropertyCollection UserProperties { get; init; }

   /// <summary>
   /// Creates a new <see cref="ClientConnectResult"/> from a <see cref="ConnAckPacket"/>.
   /// </summary>
   public static ClientConnectResult Create(in ConnAckPacket packet)
   {
      var userProperties = new List<KeyValuePair<string, string>>();

      uint? sessionExpiryInterval = null;
      ushort? serverKeepAlive = null;
      ushort? topicAliasMaximum = null;
      uint? maximumPacketSize = null;
      ushort? receiveMaximum = null;
      QualityOfServiceType? maximumQualityOfService = null;
      bool? isRetainAvailable = null;
      bool? isSharedSubscriptionAvailable = null;
      bool? isSubscriptionIdentifierAvailable = null;
      bool? isWildcardSubscriptionAvailable = null;

      if (!packet.PropertiesBytes.IsEmpty)
      {
         var enumerator = packet.GetProperties();
         while (enumerator.MoveNext())
         {
            switch (enumerator.Current.Identifier)
            {
               case PropertyIdentifier.SessionExpiryInterval:
                  sessionExpiryInterval = enumerator.Current.AsSessionExpiryInterval();
                  break;
               case PropertyIdentifier.ServerKeepAlive:
                  serverKeepAlive = enumerator.Current.AsServerKeepAlive();
                  break;
               case PropertyIdentifier.TopicAliasMaximum:
                  topicAliasMaximum = enumerator.Current.AsTopicAliasMaximum();
                  break;
               case PropertyIdentifier.MaximumPacketSize:
                  maximumPacketSize = enumerator.Current.AsFourByteInteger();
                  break;
               case PropertyIdentifier.ReceiveMaximum:
                  receiveMaximum = enumerator.Current.AsReceiveMaximum();
                  break;
               case PropertyIdentifier.MaximumQos:
                  var maxQosRes = enumerator.Current.AsMaximumQualityOfService();
                  if (maxQosRes.IsSuccess)
                  {
                     maximumQualityOfService = maxQosRes.Success;
                  }
                  break;
               case PropertyIdentifier.RetainAvailable:
                  isRetainAvailable = enumerator.Current.AsRetainAvailable();
                  break;
               case PropertyIdentifier.SharedSubscriptionAvailable:
                  isSharedSubscriptionAvailable = enumerator.Current.AsSharedSubscriptionAvailable();
                  break;
               case PropertyIdentifier.SubscriptionIdentifierAvailable:
                  isSubscriptionIdentifierAvailable = enumerator.Current.AsSubscriptionIdentifierAvailable();
                  break;
               case PropertyIdentifier.WildcardSubscriptionAvailable:
                  isWildcardSubscriptionAvailable = enumerator.Current.AsWildcardSubscriptionAvailable();
                  break;
               case PropertyIdentifier.UserProperty:
                  break;
            }
         }
      }

      return new ClientConnectResult
      {
         IsSessionPresent = packet.IsSessionPresent,
         ReturnCode = packet.ReturnCode,
         ReasonCode = packet.ReasonCode,
         ReasonString = packet.ReasonStringUtf8Bytes.GetUtf8String(),
         IsRetainAvailable = isRetainAvailable,
         IsSharedSubscriptionAvailable = isSharedSubscriptionAvailable,
         IsSubscriptionIdentifierAvailable = isSubscriptionIdentifierAvailable,
         IsWildcardSubscriptionAvailable = isWildcardSubscriptionAvailable,
         MaximumQualityOfService = maximumQualityOfService,
         SessionExpiryInterval = sessionExpiryInterval,
         ServerKeepAlive = serverKeepAlive,
         TopicAliasMaximum = topicAliasMaximum,
         MaximumPacketSize = maximumPacketSize,
         ReceiveMaximum = receiveMaximum,
         AuthenticationMethod = packet.AuthenticationMethodUtf8Bytes.GetUtf8String(),
         AuthenticationData = packet.AuthenticationDataBytes.ToNullableMemory(),
         ServerReference = packet.ServerReferenceUtf8Bytes.GetUtf8String(),
         ResponseInfo = packet.ResponseInfoUtf8Bytes.GetUtf8String(),
         AssignedClientIdentifier = packet.AssignedClientIdentifierUtf8Bytes.GetUtf8String(),
         UserProperties = UserPropertyCollection.Create(packet.PropertiesBytes)
      };
   }
}
