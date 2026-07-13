using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteConnect(ConnectOptions options)
   {
      var length = CalculateLength(options);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(options);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Connect, 0, remainingLength);

         // Protocol Name & Level
         writer.WriteBigEndian((ushort)4);
         writer.WriteBytes("MQTT"u8);
         writer.WriteByte(5);

         byte connectFlags = 0;
         if (options.CleanSession)
         {
            connectFlags |= 0x02;
         }

         if (options.HasWill)
         {
            connectFlags |= 0x04;
            connectFlags |= (byte)((byte)options.WillQualityOfService << 3);
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

         var propertiesLength = CalculatePropertiesLength(options);
         PacketEncoder.WriteVariableByteInteger(ref writer, (uint)propertiesLength);

         if (propertiesLength > 0)
         {
            var propEncoder = writer.AsConnectPropertyEncoder();

            try
            {
               if (options.SessionExpiryInterval.HasValue && options.SessionExpiryInterval.Value != 0)
               {
                  propEncoder.WriteSessionExpiryInterval(options.SessionExpiryInterval.Value);
               }

               if (options.TopicAliasMaximum.HasValue && options.TopicAliasMaximum.Value != 0)
               {
                  propEncoder.WriteTopicAliasMaximum(options.TopicAliasMaximum.Value);
               }

               if (options.MaximumPacketSize.HasValue && options.MaximumPacketSize.Value != 0)
               {
                  propEncoder.WriteMaximumPacketSize(options.MaximumPacketSize.Value);
               }

               if (options.RequestResponseInformation)
               {
                  propEncoder.WriteRequestResponseInformation(options.RequestResponseInformation);
               }

               if (!options.RequestProblemInformation)
               {
                  propEncoder.WriteRequestProblemInformation(options.RequestProblemInformation);
               }

               if (!options.AuthenticationMethodUtf8Bytes.IsEmpty)
               {
                  propEncoder.WriteAuthenticationMethod(options.AuthenticationMethodUtf8Bytes.Span);
               }

               if (!options.AuthenticationDataBytes.IsEmpty)
               {
                  propEncoder.WriteAuthenticationData(options.AuthenticationDataBytes.Span);
               }

               if (options.UserProperties.Count > 0)
               {
                  var enumerator = options.UserProperties.GetEnumerator();
                  while (enumerator.MoveNext())
                  {
                     var prop = enumerator.Current;
                     propEncoder.WriteUserProperty(prop.KeyUtf8Bytes, prop.ValueBytes);
                  }
               }
            }
            finally
            {
               writer = propEncoder.Encoder.Writer;
            }
         }

         writer.WriteBigEndian((ushort)options.ClientIdUtf8Bytes.Length);
         if (!options.ClientIdUtf8Bytes.IsEmpty)
         {
            writer.WriteBytes(options.ClientIdUtf8Bytes.Span);
         }

         if (options.HasWill)
         {
            var willPropertiesLength = CalculateWillPropertiesLength(options);
            PacketEncoder.WriteVariableByteInteger(ref writer, (uint)willPropertiesLength);

            if (willPropertiesLength > 0)
            {
               var propEncoder = writer.AsWillPropertyEncoder();

               try
               {
                  if (options.WillDelayInterval.HasValue && options.WillDelayInterval.Value != 0)
                  {
                     propEncoder.WriteWillDelayInterval(options.WillDelayInterval.Value);
                  }

                  if (options.WillPayloadFormatIndicator is not PayloadFormat.Unspecified)
                  {
                     propEncoder.WritePayloadFormatIndicator(options.WillPayloadFormatIndicator);
                  }

                  if (options.WillMessageExpiryInterval.HasValue && options.WillMessageExpiryInterval.Value != 0)
                  {
                     propEncoder.WriteMessageExpiryInterval(options.WillMessageExpiryInterval.Value);
                  }

                  if (!options.WillContentTypeUtf8Bytes.IsEmpty)
                  {
                     propEncoder.WriteContentType(options.WillContentTypeUtf8Bytes.Span);
                  }

                  if (!options.WillResponseTopicUtf8Bytes.IsEmpty)
                  {
                     propEncoder.WriteResponseTopic(options.WillResponseTopicUtf8Bytes.Span);
                  }

                  if (!options.WillCorrelationDataBytes.IsEmpty)
                  {
                     propEncoder.WriteCorrelationData(options.WillCorrelationDataBytes.Span);
                  }

                  if (options.WillUserProperties.Count > 0)
                  {
                     var enumerator = options.WillUserProperties.GetEnumerator();
                     while (enumerator.MoveNext())
                     {
                        var prop = enumerator.Current;
                        propEncoder.WriteUserProperty(prop.KeyUtf8Bytes, prop.ValueBytes);
                     }
                  }
               }
               finally
               {
                  writer = propEncoder.Encoder.Writer;
               }
            }

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
      // Protocol Name & Level = 7 bytes
      // Connect Flags (1 byte) + Keep Alive (2 bytes) = 3 bytes
      var len = 10;

      // Connect Properties
      var propertiesLength = CalculatePropertiesLength(options);
      len += PacketEncoder.GetVariableByteIntegerLength(propertiesLength) + propertiesLength;

      // Client Identifier
      len += 2 + options.ClientIdUtf8Bytes.Length;

      // Will fields
      if (options.HasWill)
      {
         var willPropertiesLength = CalculateWillPropertiesLength(options);
         len += PacketEncoder.GetVariableByteIntegerLength(willPropertiesLength) + willPropertiesLength;
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

   private static int CalculatePropertiesLength(ConnectOptions options)
   {
      var len = 0;

      if (options.SessionExpiryInterval.HasValue && options.SessionExpiryInterval.Value != 0)
      {
         len += 5;
      }

      if (options.TopicAliasMaximum.HasValue && options.TopicAliasMaximum.Value != 0)
      {
         len += 3;
      }

      if (options.MaximumPacketSize.HasValue && options.MaximumPacketSize.Value != 0)
      {
         len += 5;
      }

      if (options.RequestResponseInformation)
      {
         len += 2;
      }

      if (!options.RequestProblemInformation)
      {
         len += 2;
      }

      if (!options.AuthenticationMethodUtf8Bytes.IsEmpty)
      {
         len += 3 + options.AuthenticationMethodUtf8Bytes.Length;
      }

      if (!options.AuthenticationDataBytes.IsEmpty)
      {
         len += 3 + options.AuthenticationDataBytes.Length;
      }

      len += options.UserProperties.ByteCount;

      return len;
   }

   private static int CalculateWillPropertiesLength(ConnectOptions options)
   {
      var len = 0;

      if (options.WillDelayInterval.HasValue && options.WillDelayInterval.Value != 0)
      {
         len += 5;
      }

      if (options.WillPayloadFormatIndicator is not PayloadFormat.Unspecified)
      {
         len += 2;
      }

      if (options.WillMessageExpiryInterval.HasValue && options.WillMessageExpiryInterval.Value != 0)
      {
         len += 5;
      }

      if (!options.WillContentTypeUtf8Bytes.IsEmpty)
      {
         len += 3 + options.WillContentTypeUtf8Bytes.Length;
      }

      if (!options.WillResponseTopicUtf8Bytes.IsEmpty)
      {
         len += 3 + options.WillResponseTopicUtf8Bytes.Length;
      }

      if (!options.WillCorrelationDataBytes.IsEmpty)
      {
         len += 3 + options.WillCorrelationDataBytes.Length;
      }

      len += options.WillUserProperties.ByteCount;

      return len;
   }
}
