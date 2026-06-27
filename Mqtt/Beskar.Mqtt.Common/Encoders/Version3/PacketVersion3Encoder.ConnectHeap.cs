using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders.Version3;

public readonly ref partial struct PacketVersion3Encoder
{
   public void WriteConnect(ConnectOptions options)
   {
      var length = CalculateLength(options);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(options);
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
         if (options.CleanSession)
         {
            connectFlags |= 0x02;
         }

         if (options.HasWill)
         {
            connectFlags |= 0x04;
            connectFlags |= (byte)((int)options.WillQualityOfService << 3);
            if (options.WillRetain)
            {
               connectFlags |= 0x20;
            }
         }

         var hasUsername = !options.UsernameUtf8Bytes.IsEmpty;
         var hasPassword = !options.PasswordBytes.IsEmpty;

         if (hasUsername)
         {
            connectFlags |= 0x80;
         }
         if (hasPassword)
         {
            connectFlags |= 0x40;
         }

         writer.WriteByte(connectFlags);
         writer.WriteBigEndian(options.KeepAlivePeriod);

         writer.WriteBigEndian((ushort)options.ClientIdUtf8Bytes.Length);
         if (!options.ClientIdUtf8Bytes.IsEmpty)
         {
            writer.WriteBytes(options.ClientIdUtf8Bytes.Span);
         }

         if (options.HasWill)
         {
            writer.WriteBigEndian((ushort)options.WillTopicUtf8Bytes.Length);
            if (!options.WillTopicUtf8Bytes.IsEmpty)
            {
               writer.WriteBytes(options.WillTopicUtf8Bytes.Span);
            }

            PacketEncoder.WriteSequence(ref writer, options.WillPayload);
         }

         if (hasUsername)
         {
            writer.WriteBigEndian((ushort)options.UsernameUtf8Bytes.Length);
            if (!options.UsernameUtf8Bytes.IsEmpty)
            {
               writer.WriteBytes(options.UsernameUtf8Bytes.Span);
            }
         }
         if (hasPassword)
         {
            writer.WriteBigEndian((ushort)options.PasswordBytes.Length);
            if (!options.PasswordBytes.IsEmpty)
            {
               writer.WriteBytes(options.PasswordBytes.Span);
            }
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private int CalculateLength(ConnectOptions options)
   {
      var remainingLength = CalculateRemainingLength(options);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private int CalculateRemainingLength(ConnectOptions options)
   {
      // Protocol Name & Level
      var len = _protocolVersion is MqttProtocolVersion.V31 ? 9 : 7;

      // Connect Flags (1 byte) + Keep Alive (2 bytes)
      len += 3;
      // Client Identifier
      len += 2 + options.ClientIdUtf8Bytes.Length;

      // Will fields
      if (options.HasWill)
      {
         len += 2 + options.WillTopicUtf8Bytes.Length;
         len += 2 + (int)options.WillPayload.Length;
      }

      // Username
      if (!options.UsernameUtf8Bytes.IsEmpty)
      {
         len += 2 + options.UsernameUtf8Bytes.Length;
      }
      // Password
      if (!options.PasswordBytes.IsEmpty)
      {
         len += 2 + options.PasswordBytes.Length;
      }

      return len;
   }
}
