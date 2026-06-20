using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public ref struct UnsubAckPacket
{
   public ushort PacketIdentifier;

   public override string ToString()
   {
      return "UNSUBACK";
   }

   internal string DebuggerDisplay => ToString();
}
