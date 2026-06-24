using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enumerators;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public ref struct UnsubAckPacket
{
   public ushort PacketIdentifier;

   public ReadOnlySequence<byte> ReasonStringUtf8Bytes;

   public ReadOnlySequence<byte> PropertiesBytes;
   public readonly MqttPropertyEnumerator GetProperties() => new(PropertiesBytes);

   public ReadOnlySequence<byte> ReasonCodesBytes;
   public readonly UnsubscribeReasonCodeEnumerator GetReasonCodes() => new(ReasonCodesBytes);

   public override string ToString()
   {
      return "UNSUBACK";
   }

   internal string DebuggerDisplay => ToString();
}
