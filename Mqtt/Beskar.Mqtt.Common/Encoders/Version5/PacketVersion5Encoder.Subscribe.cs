using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteSubscribe(in SubscribePacket packet)
   {
      var existingMask = CollectPresentProperties(packet.PropertiesBytes);
      var propsDataLen = CalculateSubscribePropertiesDataLength(in packet, existingMask);
      var remainingLength = 2
         + PacketEncoder.GetVariableByteIntegerLength(propsDataLen)
         + propsDataLen
         + (int)packet.FiltersBytes.Length;
      var length = PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Subscribe, 2, remainingLength);
         writer.WriteBigEndian(packet.PacketIdentifier);

         PacketEncoder.WriteVariableByteInteger(ref writer, (uint)propsDataLen);
         if (propsDataLen > 0)
         {
            var propEncoder = writer.AsSubscribePropertyEncoder();
            try
            {
               if (packet.SubscriptionIdentifier != 0
                   && !HasProperty(existingMask, PropertyIdentifier.SubscriptionIdentifier))
               {
                  propEncoder.WriteSubscriptionIdentifier(packet.SubscriptionIdentifier);
               }
            }
            finally
            {
               writer = propEncoder.Encoder.Writer;
            }

            PacketEncoder.WriteRawSequence(ref writer, packet.PropertiesBytes);
         }

         PacketEncoder.WriteRawSequence(ref writer, packet.FiltersBytes);

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static int CalculateSubscribePropertiesDataLength(in SubscribePacket packet, ulong existingMask)
   {
      var len = (int)packet.PropertiesBytes.Length;
      if (packet.SubscriptionIdentifier != 0
          && !HasProperty(existingMask, PropertyIdentifier.SubscriptionIdentifier))
      {
         len += 1 + PacketEncoder.GetVariableByteIntegerLength((int)packet.SubscriptionIdentifier);
      }

      return len;
   }
}
