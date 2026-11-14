using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public struct ConnAckPacket() : IRawMqttPacket
{
   public bool IsSessionPresent;
   public ConnectReturnCode ReturnCode;
   public ConnectReasonCode ReasonCode;
   public ReadOnlySequence<byte> ReasonStringUtf8Bytes;

   public bool IsRetainAvailable = true;
   public bool IsSharedSubscriptionAvailable = true;
   public bool IsSubscriptionIdentifierAvailable = true;
   public bool IsWildcardSubscriptionAvailable = true;

   public QualityOfServiceType MaximumQualityOfService = QualityOfServiceType.ExactlyOnce;

   public uint SessionExpiryInterval;
   public ushort ServerKeepAlive;
   public ushort TopicAliasMaximum;
   public uint MaximumPacketSize;
   public ushort ReceiveMaximum;

   public ReadOnlySequence<byte> AuthenticationMethodUtf8Bytes;
   public ReadOnlySequence<byte> AuthenticationDataBytes;

   public ReadOnlySequence<byte> ServerReferenceUtf8Bytes;
   public ReadOnlySequence<byte> ResponseInfoUtf8Bytes;
   public ReadOnlySequence<byte> AssignedClientIdentifierUtf8Bytes;

   public ReadOnlySequence<byte> PropertiesBytes;
   public readonly MqttPropertyEnumerator GetProperties() => new(PropertiesBytes);

   public override string ToString()
   {
      return "CONNACK";
   }

   internal string DebuggerDisplay => ToString();
}
