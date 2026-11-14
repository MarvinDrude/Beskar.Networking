using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteSubAck(in SubAckPacket packet)
   {
      var propsDataLen = CalculateReasonPropertiesDataLength(packet.ReasonStringUtf8Bytes, packet.PropertiesBytes);
      var remainingLength = 2
         + PacketEncoder.GetVariableByteIntegerLength(propsDataLen)
         + propsDataLen
         + packet.ReturnCodesBytes.Length;
      var length = PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.SubAck, 0, remainingLength);
         writer.WriteBigEndian(packet.PacketIdentifier);

         WriteReasonProperties(ref writer, packet.ReasonStringUtf8Bytes, packet.PropertiesBytes, propsDataLen);

         writer.WriteBytes(packet.ReturnCodesBytes.Span);

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }
}
