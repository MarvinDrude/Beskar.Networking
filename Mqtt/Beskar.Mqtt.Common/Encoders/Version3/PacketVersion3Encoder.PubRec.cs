using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version3;

public readonly ref partial struct PacketVersion3Encoder
{
   public void WritePubRec(in PubRecPacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.PubRec, 0, 2);
         writer.WriteBigEndian(packet.PacketIdentifier);

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static int CalculateLength(in PubRecPacket packet)
   {
      const int remainingLength = 2; // always fixed
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }
}
