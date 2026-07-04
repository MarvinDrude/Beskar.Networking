using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Interfaces;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public struct UnsubAckPacket : IRawMqttPacket
{
   public ushort PacketIdentifier;

   public ReadOnlyMemory<byte> ReasonStringUtf8Bytes;

   public ReadOnlyMemory<byte> PropertiesBytes;
   public readonly MqttPropertyEnumerator GetProperties() => new(new ReadOnlySequence<byte>(PropertiesBytes));

   public ReadOnlyMemory<byte> ReasonCodesBytes;
   public readonly UnsubscribeReasonCodeEnumerator GetReasonCodes() => new(new ReadOnlySequence<byte>(ReasonCodesBytes));

   public override string ToString()
   {
      return "UNSUBACK";
   }

   internal string DebuggerDisplay => ToString();
}
