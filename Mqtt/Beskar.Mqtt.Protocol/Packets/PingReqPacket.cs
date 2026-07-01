using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Interfaces;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public struct PingReqPacket : IRawMqttPacket
{
   public override string ToString()
   {
      return "PING_REQ";
   }

   internal string DebuggerDisplay => ToString();
}
