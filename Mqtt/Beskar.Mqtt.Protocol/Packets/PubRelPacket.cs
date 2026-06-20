using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public ref struct PubRelPacket
{
   public ushort PacketIdentifier;

   public override string ToString()
   {
      return "PUBREL";
   }

   internal string DebuggerDisplay => ToString();
}
