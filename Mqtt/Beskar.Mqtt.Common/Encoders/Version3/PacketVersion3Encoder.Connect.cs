using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version3;

public readonly ref partial struct PacketVersion3Encoder
{
   public void WriteConnect(in ConnectPacket packet)
   {
      var length = CalculateLength(packet);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(packet);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Connect, 0, remainingLength);

         if (_protocolVersion is MqttProtocolVersion.V31)
         {
            writer.WriteBigEndian((ushort)6);
            writer.WriteBytes("MQIsdp"u8);
            writer.WriteByte(3);
         }
         else // MQTT v3.1.1 (default)
         {
            writer.WriteBigEndian((ushort)4);
            writer.WriteBytes("MQTT"u8);
            writer.WriteByte(4);
         }

         byte connectFlags = 0;
         if (packet.IsCleanSession)
         {
            connectFlags |= 0x02;
         }

         if (packet.HasWill)
         {
            connectFlags |= 0x04;
            connectFlags |= (byte)((int)packet.WillQualityOfService << 3);
            if (packet.WillRetain)
            {
               connectFlags |= 0x20;
            }
         }

         var hasUsername = packet.UsernameUtf8Bytes.Length > 0;
         var hasPassword = packet.PasswordBytes.Length > 0;

         if (hasUsername)
         {
            connectFlags |= 0x80;
         }
         if (hasPassword)
         {
            connectFlags |= 0x40;
         }

         writer.WriteByte(connectFlags);
         writer.WriteBigEndian(packet.KeepAliveInterval);

         PacketEncoder.WriteSequence(ref writer, packet.ClientIdUtf8Bytes);

         if (packet.HasWill)
         {
            PacketEncoder.WriteSequence(ref writer, packet.WillTopicUtf8Bytes);
            PacketEncoder.WriteSequence(ref writer, packet.WillMessageBytes);
         }

         if (hasUsername)
         {
            PacketEncoder.WriteSequence(ref writer, packet.UsernameUtf8Bytes);
         }
         if (hasPassword)
         {
            PacketEncoder.WriteSequence(ref writer, packet.PasswordBytes);
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private int CalculateLength(in ConnectPacket packet)
   {
      var remainingLength = CalculateRemainingLength(packet);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private int CalculateRemainingLength(in ConnectPacket packet)
   {
      // Protocol Name & Level
      var len = _protocolVersion is MqttProtocolVersion.V31 ? 9 : 7;

      // Connect Flags (1 byte) + Keep Alive (2 bytes)
      len += 3;
      // Client Identifier
      len += 2 + (int)packet.ClientIdUtf8Bytes.Length;

      // Will fields
      if (packet.HasWill)
      {
         len += 2 + (int)packet.WillTopicUtf8Bytes.Length;
         len += 2 + (int)packet.WillMessageBytes.Length;
      }

      // Username
      if (packet.UsernameUtf8Bytes.Length > 0)
      {
         len += 2 + (int)packet.UsernameUtf8Bytes.Length;
      }
      // Password
      if (packet.PasswordBytes.Length > 0)
      {
         len += 2 + (int)packet.PasswordBytes.Length;
      }

      return len;
   }
}
