using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public struct DisconnectPacket : IRawMqttPacket
{
   public DisconnectReasonCode ReasonCode;
   public ReadOnlySequence<byte> ReasonUtf8Bytes;

   public ReadOnlySequence<byte> ServerReferenceUtf8Bytes;
   public uint SessionExpiryInterval;

   public ReadOnlySequence<byte> PropertiesBytes;
   public readonly MqttPropertyEnumerator GetProperties() => new(PropertiesBytes);

   public override string ToString()
   {
      return "DISCONNECT";
   }

   internal string DebuggerDisplay => ToString();
}
