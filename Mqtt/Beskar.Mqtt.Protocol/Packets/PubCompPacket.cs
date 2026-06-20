using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public struct PubCompPacket
{
   public ushort PacketIdentifier;

   public override string ToString()
   {
      return "PUBCOMP";
   }

   internal string DebuggerDisplay => ToString();
}
