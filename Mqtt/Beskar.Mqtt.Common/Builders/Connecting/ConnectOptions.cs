using System;
using System.Buffers;
using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Builders.Connecting;

public sealed class ConnectOptions(int builderCapacity = -1)
   : UserPropertiesBaseOptions(builderCapacity)
{
   private readonly int _builderCapacity = builderCapacity;

   public MqttProtocolVersion ProtocolVersion { get; set; } = MqttProtocolVersion.V50;

   public bool CleanSession { get; set; } = true;
   public ushort KeepAlivePeriod { get; set; } = 60;
   public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(15);

   public ReadOnlyMemory<byte> ClientIdUtf8Bytes { get; set; }
   public ReadOnlyMemory<byte> UsernameUtf8Bytes { get; set; }
   public ReadOnlyMemory<byte> PasswordBytes { get; set; }

   public uint? SessionExpiryInterval { get; set; }
   public ushort? TopicAliasMaximum { get; set; }
   public uint? MaximumPacketSize { get; set; }

   public bool RequestResponseInformation { get; set; }
   public bool RequestProblemInformation { get; set; } = true;

   public ReadOnlyMemory<byte> AuthenticationMethodUtf8Bytes { get; set; }
   public ReadOnlyMemory<byte> AuthenticationDataBytes { get; set; }

   public bool TryPrivate { get; set; }
   public bool HasWill { get; set; }

   public ReadOnlyMemory<byte> WillTopicUtf8Bytes { get; set; }
   public ReadOnlySequence<byte> WillPayload { get; set; }

   public QualityOfServiceType WillQualityOfService { get; set; } = QualityOfServiceType.AtMostOnce;
   public bool WillRetain { get; set; }
   public uint? WillDelayInterval { get; set; }

   public PayloadFormat WillPayloadFormatIndicator { get; set; } = PayloadFormat.Unspecified;
   public uint? WillMessageExpiryInterval { get; set; }

   public ReadOnlyMemory<byte> WillContentTypeUtf8Bytes { get; set; }
   public ReadOnlyMemory<byte> WillResponseTopicUtf8Bytes { get; set; }
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

   public static ConnectOptionsBuilder Create() => new();
}
