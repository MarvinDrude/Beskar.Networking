using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public struct PubCompPacket : IRawMqttPacket
{
   public ushort PacketIdentifier;
   public PubCompReasonCode ReasonCode;

   public ReadOnlySequence<byte> ReasonStringUtf8Bytes;

   public ReadOnlySequence<byte> PropertiesBytes;
   public readonly MqttPropertyEnumerator GetProperties() => new(PropertiesBytes);

   public override string ToString()
   {
      return "PUBCOMP";
   }

   internal string DebuggerDisplay => ToString();
}
