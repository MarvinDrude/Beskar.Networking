using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WritePingReq(in PingReqPacket packet)
   {
      var writer = new ByteWriter(_writer.GetSpan(2));

      try
      {
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.PingReq, 0, 0);
         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   public void WritePingResp(in PingRespPacket packet)
   {
      var writer = new ByteWriter(_writer.GetSpan(2));

      try
      {
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.PingResp, 0, 0);
         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }
}
