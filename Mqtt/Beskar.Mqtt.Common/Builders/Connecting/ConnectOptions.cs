using System.Buffers;
using System.Net;
using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Builders.Connecting;

/// <summary>
/// All options that are available for sending a CONNECT packet in MQTT.
/// </summary>
public sealed class ConnectOptions(int builderCapacity = -1)
   : UserPropertiesBaseOptions(builderCapacity)
{
   private readonly int _builderCapacity = builderCapacity;

   /// <summary>
   /// The endpoint of the MQTT Server to connect to.
   /// </summary>
   public required IPEndPoint EndPoint { get; set; }

   /// <summary>
   /// The MQTT protocol version to use for the connection.
   /// </summary>
   public MqttProtocolVersion ProtocolVersion { get; set; } = MqttProtocolVersion.V50;

   /// <summary>
   /// Whether to start a clean session/start.
   /// </summary>
   public bool CleanSession { get; set; } = true;

   /// <summary>
   /// The keep alive period in seconds.
   /// </summary>
   public ushort KeepAlivePeriod { get; set; } = 60;

   /// <summary>
   /// The timeout for the network connection attempt.
   /// </summary>
   public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(30);

   /// <summary>
   /// The Client Identifier.
   /// </summary>
   public ReadOnlyMemory<byte> ClientIdUtf8Bytes { get; set; }

   /// <summary>
   /// The Username.
   /// </summary>
   public ReadOnlyMemory<byte> UsernameUtf8Bytes { get; set; }

   /// <summary>
   /// The Password.
   /// </summary>
   public ReadOnlyMemory<byte> PasswordBytes { get; set; }

   /// <summary>
   /// The Session Expiry Interval in seconds.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public uint? SessionExpiryInterval { get; set; }

   /// <summary>
   /// The Topic Alias Maximum.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ushort? TopicAliasMaximum { get; set; }

   /// <summary>
   /// The Receive Maximum.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ushort? ReceiveMaximum { get; set; }

   /// <summary>
   /// The Maximum Packet Size.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public uint? MaximumPacketSize { get; set; }

   /// <summary>
   /// The Request Response Information.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public bool RequestResponseInformation { get; set; }

   /// <summary>
   /// The Request Problem Information.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public bool RequestProblemInformation { get; set; } = true;

   /// <summary>
   /// The Authentication Method.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ReadOnlyMemory<byte> AuthenticationMethodUtf8Bytes { get; set; }

   /// <summary>
   /// The Authentication Data.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ReadOnlyMemory<byte> AuthenticationDataBytes { get; set; }

   /// <summary>
   /// Tries to establish a private bridge connection (Mosquitto/MQTT 3.1.1 Bridge flag).
   /// </summary>
   public bool TryPrivate { get; set; }

   /// <summary>
   /// Whether a Will Message is configured.
   /// </summary>
   public bool HasWill { get; set; }

   /// <summary>
   /// The Will Topic.
   /// </summary>
   public ReadOnlyMemory<byte> WillTopicUtf8Bytes { get; set; }

   /// <summary>
   /// The Will Payload.
   /// </summary>
   public ReadOnlySequence<byte> WillPayload { get; set; }

   /// <summary>
   /// The Will Quality of Service.
   /// </summary>
   public QualityOfServiceType WillQualityOfService { get; set; } = QualityOfServiceType.AtMostOnce;

   /// <summary>
   /// Whether the Will Message should be retained.
   /// </summary>
   public bool WillRetain { get; set; }

   /// <summary>
   /// The Will Delay Interval.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public uint? WillDelayInterval { get; set; }

   /// <summary>
   /// The Will Payload Format Indicator.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PayloadFormat WillPayloadFormatIndicator { get; set; } = PayloadFormat.Unspecified;

   /// <summary>
   /// The Will Message Expiry Interval.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public uint? WillMessageExpiryInterval { get; set; }

   /// <summary>
   /// The Will Content Type.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ReadOnlyMemory<byte> WillContentTypeUtf8Bytes { get; set; }

   /// <summary>
   /// The Will Response Topic.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ReadOnlyMemory<byte> WillResponseTopicUtf8Bytes { get; set; }

   /// <summary>
   /// The Will Correlation Data.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public ReadOnlyMemory<byte> WillCorrelationDataBytes { get; set; }

   /// <summary>
   /// Key-Value pairs by the user for the will.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public UserPropertyListBuilder WillUserProperties
      => field ??= new UserPropertyListBuilder(_builderCapacity == -1 ? 128 :  _builderCapacity);

   /// <summary>
   /// Provider used to fill in username and password in CONNECT,
   /// Overwrites the username and password properties in here.
   /// </summary>
   public IMqttCredentialsProvider? CredentialsProvider { get; set; }

   /// <summary>
   /// Used to control how the auth flow should interact with the
   /// auth challenge from the server.
   /// </summary>
   public IMqttAuthenticationHandler? AuthenticationHandler { get; set; }

   public override void Clear()
   {
      base.Clear();
      WillUserProperties.Clear();

      ProtocolVersion = MqttProtocolVersion.V50;
      CleanSession = true;
      KeepAlivePeriod = 60;
      Timeout = TimeSpan.FromSeconds(30);

      ClientIdUtf8Bytes = ReadOnlyMemory<byte>.Empty;
      UsernameUtf8Bytes = ReadOnlyMemory<byte>.Empty;
      PasswordBytes = ReadOnlyMemory<byte>.Empty;

      SessionExpiryInterval = null;
      TopicAliasMaximum = null;
      ReceiveMaximum = null;
      MaximumPacketSize = null;
      RequestResponseInformation = false;
      RequestProblemInformation = true;

      AuthenticationMethodUtf8Bytes = ReadOnlyMemory<byte>.Empty;
      AuthenticationDataBytes = ReadOnlyMemory<byte>.Empty;
      TryPrivate = false;

      HasWill = false;
      WillTopicUtf8Bytes = ReadOnlyMemory<byte>.Empty;
      WillPayload = ReadOnlySequence<byte>.Empty;
      WillQualityOfService = QualityOfServiceType.AtMostOnce;

      WillRetain = false;
      WillDelayInterval = null;
      WillPayloadFormatIndicator = PayloadFormat.Unspecified;
      WillMessageExpiryInterval = null;

      WillContentTypeUtf8Bytes = ReadOnlyMemory<byte>.Empty;
      WillResponseTopicUtf8Bytes = ReadOnlyMemory<byte>.Empty;
      WillCorrelationDataBytes = ReadOnlyMemory<byte>.Empty;

      CredentialsProvider = null;
      AuthenticationHandler = null;
   }

   /// <summary>
   /// Creates a new ConnectOptionsBuilder.
   /// </summary>
   public static ConnectOptionsBuilder Create(IPEndPoint endPoint) => new(endPoint);

    /// <summary>
    /// Create a connect heap packet version of a stack instance
    /// </summary>
    public static ConnectOptions Create(in ConnectPacket packet, MqttProtocolVersion protocolVersion,
       IPEndPoint endPoint)
    {
       var options = new ConnectOptions
       {
          EndPoint = endPoint,
          ProtocolVersion = protocolVersion,
          CleanSession = packet.IsCleanSession,
          KeepAlivePeriod = packet.KeepAliveInterval,
          ClientIdUtf8Bytes = packet.ClientIdUtf8Bytes.ToArray(),
          UsernameUtf8Bytes = packet.UsernameUtf8Bytes.ToArray(),
          PasswordBytes = packet.PasswordBytes.ToArray()
       };

       if (!packet.PropertiesBytes.IsEmpty)
       {
          var propEnumerator = packet.GetProperties();
          while (propEnumerator.MoveNext())
          {
             switch (propEnumerator.Current.Identifier)
             {
                case PropertyIdentifier.SessionExpiryInterval:
                   options.SessionExpiryInterval = propEnumerator.Current.AsSessionExpiryInterval();
                   break;
                case PropertyIdentifier.TopicAliasMaximum:
                   options.TopicAliasMaximum = propEnumerator.Current.AsTopicAliasMaximum();
                   break;
                case PropertyIdentifier.ReceiveMaximum:
                   options.ReceiveMaximum = propEnumerator.Current.AsReceiveMaximum();
                   break;
                case PropertyIdentifier.MaximumPacketSize:
                   options.MaximumPacketSize = propEnumerator.Current.AsMaximumPacketSize();
                   break;
                case PropertyIdentifier.RequestResponseInformation:
                   options.RequestResponseInformation = propEnumerator.Current.AsRequestResponseInfo();
                   break;
                case PropertyIdentifier.RequestProblemInformation:
                   options.RequestProblemInformation = propEnumerator.Current.AsRequestProblemInfo();
                   break;
                case PropertyIdentifier.AuthenticationMethod:
                   options.AuthenticationMethodUtf8Bytes = propEnumerator.Current.AsAuthenticationMethod().ToArray();
                   break;
                case PropertyIdentifier.AuthenticationData:
                   options.AuthenticationDataBytes = propEnumerator.Current.AsAuthenticationData().ToArray();
                   break;
                case PropertyIdentifier.UserProperty:
                   var pair = propEnumerator.Current.AsUserProperty();
                   options.UserProperties.Add(pair.KeyBytes.ToArray(), pair.ValueBytes.ToArray());
                   break;
             }
          }
       }

       if (packet.HasWill)
       {
          options.HasWill = true;
          options.WillTopicUtf8Bytes = packet.WillTopicUtf8Bytes.ToArray();
          options.WillPayload = new ReadOnlySequence<byte>(packet.WillMessageBytes.ToArray());
          options.WillQualityOfService = packet.WillQualityOfService;
          options.WillRetain = packet.WillRetain;

          if (!packet.WillPropertiesBytes.IsEmpty)
          {
             var enumerator = packet.GetWillProperties();
             while (enumerator.MoveNext())
             {
                switch (enumerator.Current.Identifier)
                {
                   case PropertyIdentifier.PayloadFormatIndicator:
                      options.WillPayloadFormatIndicator = enumerator.Current.AsPayloadFormat();
                      break;
                   case PropertyIdentifier.MessageExpiryInterval:
                      options.WillMessageExpiryInterval = enumerator.Current.AsMessageExpiryInterval();
                      break;
                   case PropertyIdentifier.WillDelayInterval:
                      options.WillDelayInterval = enumerator.Current.AsWillDelayInterval();
                      break;
                   case PropertyIdentifier.ContentType:
                      options.WillContentTypeUtf8Bytes = enumerator.Current.AsContentType().ToArray();
                      break;
                   case PropertyIdentifier.CorrelationData:
                      options.WillCorrelationDataBytes = enumerator.Current.AsCorrelationData().ToArray();
                      break;
                   case PropertyIdentifier.ResponseTopic:
                      options.WillResponseTopicUtf8Bytes = enumerator.Current.AsResponseTopic().ToArray();
                      break;
                   case PropertyIdentifier.UserProperty:
                      var pair = enumerator.Current.AsUserProperty();
                      options.WillUserProperties.Add(pair.KeyBytes.ToArray(), pair.ValueBytes.ToArray());
                      break;
                }
             }
          }
       }

       return options;
    }
}
