using System.Buffers;
using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Builders.Connecting;

/// <summary>
/// All options that are available for sending a CONNECT packet in MQTT.
/// </summary>
public sealed class ConnectOptions(int builderCapacity = -1)
   : UserPropertiesBaseOptions(builderCapacity)
{
   private readonly int _builderCapacity = builderCapacity;

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
   public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(15);

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

   public override void Clear()
   {
      base.Clear();
      WillUserProperties.Clear();

      ProtocolVersion = MqttProtocolVersion.V50;
      CleanSession = true;
      KeepAlivePeriod = 60;
      Timeout = TimeSpan.FromSeconds(15);

      ClientIdUtf8Bytes = ReadOnlyMemory<byte>.Empty;
      UsernameUtf8Bytes = ReadOnlyMemory<byte>.Empty;
      PasswordBytes = ReadOnlyMemory<byte>.Empty;

      SessionExpiryInterval = null;
      TopicAliasMaximum = null;
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
   }

   /// <summary>
   /// Creates a new ConnectOptionsBuilder.
   /// </summary>
   public static ConnectOptionsBuilder Create() => new();
}
