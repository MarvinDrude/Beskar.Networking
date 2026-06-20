using System.Diagnostics;
using System.Runtime.InteropServices;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public ref struct PingReqPacket
{
   public override string ToString()
   {
      return "PING_REQ";
   }

   internal string DebuggerDisplay => ToString();
}
