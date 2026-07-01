using System.Buffers;
using System.Diagnostics;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Interfaces;

namespace Beskar.Mqtt.Protocol.Packets;

[StructLayout(LayoutKind.Auto)]
[DebuggerDisplay("{DebuggerDisplay,nq}")]
public struct SubAckPacket : IRawMqttPacket
{
   public ushort PacketIdentifier;
   public ReadOnlySequence<byte> ReturnCodesBytes;

   public ReadOnlySequence<byte> ReasonStringUtf8Bytes;

   public ReadOnlySequence<byte> PropertiesBytes;
   public readonly MqttPropertyEnumerator GetProperties() => new(PropertiesBytes);

   public override string ToString()
   {
      return "SUBACK";
   }

   internal string DebuggerDisplay => ToString();

   public readonly SubscribeReasonCodeEnumerator GetReturnCodes() => new(ReturnCodesBytes);
}
