using System.Buffers.Binary;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version3;

public readonly ref partial struct PacketVersion3Encoder
{
   public void WritePubAck(in PubAckPacket packet)
   {
      var length = CalculateLength(packet);
      using var writer = new ByteWriter(_writer.GetSpan(length));



      WriteFixedHeader(MqttPacketType.PubAck, 0, remainingLength);

      var span = _writer.GetSpan(remainingLength);
      BinaryPrimitives.WriteUInt16BigEndian(span, packet.PacketIdentifier);
      _writer.Advance(remainingLength);
   }

   public int CalculateLength(in PubAckPacket packet)
   {
      const int remainingLength = 2; // always fixed
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }
}

