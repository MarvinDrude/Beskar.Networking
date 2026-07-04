using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders.Version3;

public ref partial struct PacketVersion3Encoder
{
   public void WriteDisconnect(DisconnectOptions options)
   {
      var writer = new ByteWriter(_writer.GetSpan(2));

      try
      {
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Disconnect, 0, 0);
         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }
}
