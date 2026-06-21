using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public ref struct ConnectPacket
{
   public bool IsCleanSession;
   public ushort KeepAliveInterval;

   public ReadOnlySequence<byte> ClientIdUtf8Bytes;

   public uint SessionExpiryInterval;
   public ReadOnlySequence<byte> AuthenticationMethodUtf8Bytes;
   public ReadOnlySequence<byte> AuthenticationDataBytes;

   public ushort ReceiveMaximum;
   public ushort TopicAliasMaximum;
   public uint MaximumPacketSize;

   public bool RequestResponseInfo;
   public bool RequestProblemInfo;

   public bool HasWill;
   public QualityOfServiceType WillQualityOfService;
   public bool WillRetain;

   public ReadOnlySequence<byte> WillTopicUtf8Bytes;
   public ReadOnlySequence<byte> WillMessageBytes;

   public ReadOnlySequence<byte> UsernameUtf8Bytes;
   public ReadOnlySequence<byte> PasswordBytes;

   public PayloadFormat WillPayloadFormatIndicator;
   public uint WillMessageExpiryInterval;
   public uint WillDelayInterval;
   public ReadOnlySequence<byte> WillResponseTopicUtf8Bytes;
   public ReadOnlySequence<byte> WillCorrelationDataBytes;
   public ReadOnlySequence<byte> WillContentTypeUtf8Bytes;

   public ReadOnlySequence<byte> PropertiesBytes;
   public MqttPropertyEnumerator GetProperties() => new(PropertiesBytes);

   public ReadOnlySequence<byte> WillPropertiesBytes;
   public MqttPropertyEnumerator GetWillProperties() => new(WillPropertiesBytes);

   public override string ToString()
   {
      return "CONNECT";
   }

   internal string DebuggerDisplay => ToString();
}
