using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public ref struct PubRelPacket
{
   public ushort PacketIdentifier;
   public PubRelReasonCode ReasonCode;

   public ReadOnlySequence<byte> PropertiesBytes;
   public MqttPropertyEnumerator GetProperties() => new(PropertiesBytes);

   public override string ToString()
   {
      return "PUBREL";
   }

   internal string DebuggerDisplay => ToString();
}
