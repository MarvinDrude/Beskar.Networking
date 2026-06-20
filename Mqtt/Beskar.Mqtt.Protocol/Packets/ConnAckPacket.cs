using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public ref struct ConnAckPacket
{
   public bool SessionPresent;
   public ConnectReturnCode ReturnCode;

   public override string ToString()
   {
      return "CONNACK";
   }

   internal string DebuggerDisplay => ToString();
}
