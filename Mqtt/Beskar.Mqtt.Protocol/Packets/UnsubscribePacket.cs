using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enumerators;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public ref struct UnsubscribePacket
{
   public ushort PacketIdentifier;
   public ReadOnlySequence<byte> FiltersBytes;

   public ReadOnlySequence<byte> PropertiesBytes;
   public MqttPropertyEnumerator GetProperties() => new(PropertiesBytes);

   public override string ToString()
   {
      return "UNSUBSCRIBE";
   }

   internal string DebuggerDisplay => ToString();

   public UnsubscribeFilterEnumerator GetFilters() => new(FiltersBytes);
}
