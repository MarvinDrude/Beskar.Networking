using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteDisconnect(in DisconnectPacket packet)
   {
      var existingMask = CollectPresentProperties(packet.PropertiesBytes);
      var propsDataLen = CalculateDisconnectPropertiesDataLength(in packet, existingMask);
      var remainingLength = CalculateRemainingLength(in packet, propsDataLen);
      var length = PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Disconnect, 0, remainingLength);

         if (remainingLength > 0)
         {
            writer.WriteByte((byte)packet.ReasonCode);
            if (propsDataLen > 0)
            {
               PacketEncoder.WriteVariableByteInteger(ref writer, (uint)propsDataLen);

               var propEncoder = writer.AsDisconnectPropertyEncoder();
               try
               {
                  if (packet.SessionExpiryInterval.HasValue
                      && !HasProperty(existingMask, PropertyIdentifier.SessionExpiryInterval))
                  {
                     propEncoder.WriteSessionExpiryInterval(packet.SessionExpiryInterval.Value);
                  }

                  if (!packet.ReasonUtf8Bytes.IsEmpty
                      && !HasProperty(existingMask, PropertyIdentifier.ReasonString))
                  {
                     propEncoder.Encoder.Write(PropertyIdentifier.ReasonString, packet.ReasonUtf8Bytes);
                  }

                  if (!packet.ServerReferenceUtf8Bytes.IsEmpty
                      && !HasProperty(existingMask, PropertyIdentifier.ServerReference))
                  {
                     propEncoder.Encoder.Write(PropertyIdentifier.ServerReference, packet.ServerReferenceUtf8Bytes);
                  }
               }
               finally
               {
                  writer = propEncoder.Encoder.Writer;
               }

               PacketEncoder.WriteRawSequence(ref writer, packet.PropertiesBytes);
            }
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static int CalculateRemainingLength(in DisconnectPacket packet, int propsDataLen)
   {
      if (packet.ReasonCode == DisconnectReasonCode.NormalDisconnection && propsDataLen == 0)
      {
         return 0;
      }

      if (propsDataLen == 0)
      {
         return 1;
      }

      return 1 + PacketEncoder.GetVariableByteIntegerLength(propsDataLen) + propsDataLen;
   }

   private static int CalculateDisconnectPropertiesDataLength(in DisconnectPacket packet, ulong existingMask)
   {
      var len = (int)packet.PropertiesBytes.Length;

      if (packet.SessionExpiryInterval.HasValue
          && !HasProperty(existingMask, PropertyIdentifier.SessionExpiryInterval))
      {
         len += 5;
      }

      if (!packet.ReasonUtf8Bytes.IsEmpty
          && !HasProperty(existingMask, PropertyIdentifier.ReasonString))
      {
         len += 3 + (int)packet.ReasonUtf8Bytes.Length;
      }

      if (!packet.ServerReferenceUtf8Bytes.IsEmpty
          && !HasProperty(existingMask, PropertyIdentifier.ServerReference))
      {
         len += 3 + (int)packet.ServerReferenceUtf8Bytes.Length;
      }

      return len;
   }
}
