using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public ref struct DisconnectPacket
{
   public DisconnectReasonCode ReasonCode;
   public ReadOnlySequence<byte> ReasonUtf8Bytes;

   public ReadOnlySequence<byte> ServerReferenceUtf8Bytes;
   public uint SessionExpiryInterval;

   public override string ToString()
   {
      return "DISCONNECT";
   }

   internal string DebuggerDisplay => ToString();
}
