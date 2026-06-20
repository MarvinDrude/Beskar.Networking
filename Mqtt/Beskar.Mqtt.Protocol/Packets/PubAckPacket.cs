using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public struct PubAckPacket
{
   public ushort PacketIdentifier;

   public override string ToString()
   {
      return "PUBACK";
   }

   internal string DebuggerDisplay => ToString();
}
