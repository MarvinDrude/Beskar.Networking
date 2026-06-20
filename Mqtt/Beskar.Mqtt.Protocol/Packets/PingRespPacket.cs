using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public ref struct PingRespPacket
{
   public override string ToString()
   {
      return "PING_RESP";
   }

   internal string DebuggerDisplay => ToString();
}
