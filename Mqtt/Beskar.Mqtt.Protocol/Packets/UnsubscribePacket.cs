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
   public readonly MqttPropertyEnumerator GetProperties() => new(PropertiesBytes);

   public readonly UnsubscribeFilterEnumerator GetFilters() => new(FiltersBytes);

   public override string ToString()
   {
      return "UNSUBSCRIBE";
   }

   internal string DebuggerDisplay => ToString();
}
