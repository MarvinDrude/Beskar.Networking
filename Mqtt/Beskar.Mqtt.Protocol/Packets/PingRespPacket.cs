using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Interfaces;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public struct PingRespPacket : IRawMqttPacket
{
   public override string ToString()
   {
      return "PING_RESP";
   }

   internal string DebuggerDisplay => ToString();
}
