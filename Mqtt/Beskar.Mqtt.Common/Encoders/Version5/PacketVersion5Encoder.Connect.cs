using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteConnect(in ConnectPacket packet)
   {
      var connectMask = CollectPresentProperties(packet.PropertiesBytes);
      var connectPropsDataLen = CalculateConnectPropertiesDataLength(in packet, connectMask);
      var willMask = packet.HasWill ? CollectPresentProperties(packet.WillPropertiesBytes) : 0UL;
      var willPropsDataLen = packet.HasWill ? CalculateWillPropertiesDataLength(in packet, willMask) : 0;

      var remainingLength = CalculateRemainingLength(in packet, connectPropsDataLen, willPropsDataLen);
      var length = PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Connect, 0, remainingLength);

         // Protocol Name & Level
         writer.WriteBigEndian((ushort)4);
         writer.WriteBytes("MQTT"u8);
         writer.WriteByte(5);

         byte connectFlags = 0;
         if (packet.IsCleanSession)
         {
            connectFlags |= 0x02;
         }

         if (packet.HasWill)
         {
            connectFlags |= 0x04;
            connectFlags |= (byte)((byte)packet.WillQualityOfService << 3);
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

         PacketEncoder.WriteVariableByteInteger(ref writer, (uint)connectPropsDataLen);
         if (connectPropsDataLen > 0)
         {
            var propEncoder = writer.AsConnectPropertyEncoder();
            try
            {
               if (packet.SessionExpiryInterval != 0
                   && !HasProperty(connectMask, PropertyIdentifier.SessionExpiryInterval))
               {
                  propEncoder.WriteSessionExpiryInterval(packet.SessionExpiryInterval);
               }

               if (packet.ReceiveMaximum != 0
                   && !HasProperty(connectMask, PropertyIdentifier.ReceiveMaximum))
               {
                  propEncoder.WriteReceiveMaximum(packet.ReceiveMaximum);
               }

               if (packet.MaximumPacketSize != 0
                   && !HasProperty(connectMask, PropertyIdentifier.MaximumPacketSize))
               {
                  propEncoder.WriteMaximumPacketSize(packet.MaximumPacketSize);
               }

               if (packet.TopicAliasMaximum != 0
                   && !HasProperty(connectMask, PropertyIdentifier.TopicAliasMaximum))
               {
                  propEncoder.WriteTopicAliasMaximum(packet.TopicAliasMaximum);
               }

               if (packet.RequestResponseInfo
                   && !HasProperty(connectMask, PropertyIdentifier.RequestResponseInformation))
               {
                  propEncoder.WriteRequestResponseInformation(true);
               }

               if (!packet.RequestProblemInfo
                   && !HasProperty(connectMask, PropertyIdentifier.RequestProblemInformation))
               {
                  propEncoder.WriteRequestProblemInformation(false);
               }

               if (!packet.AuthenticationMethodUtf8Bytes.IsEmpty
                   && !HasProperty(connectMask, PropertyIdentifier.AuthenticationMethod))
               {
                  propEncoder.Encoder.Write(PropertyIdentifier.AuthenticationMethod, packet.AuthenticationMethodUtf8Bytes);
               }

               if (!packet.AuthenticationDataBytes.IsEmpty
                   && !HasProperty(connectMask, PropertyIdentifier.AuthenticationData))
               {
                  propEncoder.Encoder.Write(PropertyIdentifier.AuthenticationData, packet.AuthenticationDataBytes);
               }
            }
            finally
            {
               writer = propEncoder.Encoder.Writer;
            }

            PacketEncoder.WriteRawSequence(ref writer, packet.PropertiesBytes);
         }

         PacketEncoder.WriteSequence(ref writer, packet.ClientIdUtf8Bytes);

         if (packet.HasWill)
         {
            PacketEncoder.WriteVariableByteInteger(ref writer, (uint)willPropsDataLen);
            if (willPropsDataLen > 0)
            {
               var willPropEncoder = writer.AsWillPropertyEncoder();
               try
               {
                  if (packet.WillDelayInterval != 0
                      && !HasProperty(willMask, PropertyIdentifier.WillDelayInterval))
                  {
                     willPropEncoder.WriteWillDelayInterval(packet.WillDelayInterval);
                  }

                  if (packet.WillPayloadFormatIndicator != PayloadFormat.Unspecified
                      && !HasProperty(willMask, PropertyIdentifier.PayloadFormatIndicator))
                  {
                     willPropEncoder.WritePayloadFormatIndicator(packet.WillPayloadFormatIndicator);
                  }

                  if (packet.WillMessageExpiryInterval != 0
                      && !HasProperty(willMask, PropertyIdentifier.MessageExpiryInterval))
                  {
                     willPropEncoder.WriteMessageExpiryInterval(packet.WillMessageExpiryInterval);
                  }

                  if (!packet.WillContentTypeUtf8Bytes.IsEmpty
                      && !HasProperty(willMask, PropertyIdentifier.ContentType))
                  {
                     willPropEncoder.Encoder.Write(PropertyIdentifier.ContentType, packet.WillContentTypeUtf8Bytes);
                  }

                  if (!packet.WillResponseTopicUtf8Bytes.IsEmpty
                      && !HasProperty(willMask, PropertyIdentifier.ResponseTopic))
                  {
                     willPropEncoder.Encoder.Write(PropertyIdentifier.ResponseTopic, packet.WillResponseTopicUtf8Bytes);
                  }

                  if (!packet.WillCorrelationDataBytes.IsEmpty
                      && !HasProperty(willMask, PropertyIdentifier.CorrelationData))
                  {
                     willPropEncoder.Encoder.Write(PropertyIdentifier.CorrelationData, packet.WillCorrelationDataBytes);
                  }
               }
               finally
               {
                  writer = willPropEncoder.Encoder.Writer;
               }

               PacketEncoder.WriteRawSequence(ref writer, packet.WillPropertiesBytes);
            }

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

   private static int CalculateRemainingLength(in ConnectPacket packet, int connectPropsDataLen, int willPropsDataLen)
   {
      var len = 10;

      len += PacketEncoder.GetVariableByteIntegerLength(connectPropsDataLen) + connectPropsDataLen;
      len += 2 + (int)packet.ClientIdUtf8Bytes.Length;

      if (packet.HasWill)
      {
         len += PacketEncoder.GetVariableByteIntegerLength(willPropsDataLen) + willPropsDataLen;
         len += 2 + (int)packet.WillTopicUtf8Bytes.Length;
         len += 2 + (int)packet.WillMessageBytes.Length;
      }

      if (packet.UsernameUtf8Bytes.Length > 0)
      {
         len += 2 + (int)packet.UsernameUtf8Bytes.Length;
      }

      if (packet.PasswordBytes.Length > 0)
      {
         len += 2 + (int)packet.PasswordBytes.Length;
      }

      return len;
   }

   private static int CalculateConnectPropertiesDataLength(in ConnectPacket packet, ulong existingMask)
   {
      var len = (int)packet.PropertiesBytes.Length;

      if (packet.SessionExpiryInterval != 0 && !HasProperty(existingMask, PropertyIdentifier.SessionExpiryInterval))
      {
         len += 5;
      }

      if (packet.ReceiveMaximum != 0 && !HasProperty(existingMask, PropertyIdentifier.ReceiveMaximum))
      {
         len += 3;
      }

      if (packet.MaximumPacketSize != 0 && !HasProperty(existingMask, PropertyIdentifier.MaximumPacketSize))
      {
         len += 5;
      }

      if (packet.TopicAliasMaximum != 0 && !HasProperty(existingMask, PropertyIdentifier.TopicAliasMaximum))
      {
         len += 3;
      }

      if (packet.RequestResponseInfo && !HasProperty(existingMask, PropertyIdentifier.RequestResponseInformation))
      {
         len += 2;
      }

      if (!packet.RequestProblemInfo && !HasProperty(existingMask, PropertyIdentifier.RequestProblemInformation))
      {
         len += 2;
      }

      if (!packet.AuthenticationMethodUtf8Bytes.IsEmpty && !HasProperty(existingMask, PropertyIdentifier.AuthenticationMethod))
      {
         len += 3 + (int)packet.AuthenticationMethodUtf8Bytes.Length;
      }

      if (!packet.AuthenticationDataBytes.IsEmpty && !HasProperty(existingMask, PropertyIdentifier.AuthenticationData))
      {
         len += 3 + (int)packet.AuthenticationDataBytes.Length;
      }

      return len;
   }

   private static int CalculateWillPropertiesDataLength(in ConnectPacket packet, ulong existingMask)
   {
      var len = (int)packet.WillPropertiesBytes.Length;

      if (packet.WillDelayInterval != 0 && !HasProperty(existingMask, PropertyIdentifier.WillDelayInterval))
      {
         len += 5;
      }

      if (packet.WillPayloadFormatIndicator != PayloadFormat.Unspecified
          && !HasProperty(existingMask, PropertyIdentifier.PayloadFormatIndicator))
      {
         len += 2;
      }

      if (packet.WillMessageExpiryInterval != 0 && !HasProperty(existingMask, PropertyIdentifier.MessageExpiryInterval))
      {
         len += 5;
      }

      if (!packet.WillContentTypeUtf8Bytes.IsEmpty && !HasProperty(existingMask, PropertyIdentifier.ContentType))
      {
         len += 3 + (int)packet.WillContentTypeUtf8Bytes.Length;
      }

      if (!packet.WillResponseTopicUtf8Bytes.IsEmpty && !HasProperty(existingMask, PropertyIdentifier.ResponseTopic))
      {
         len += 3 + (int)packet.WillResponseTopicUtf8Bytes.Length;
      }

      if (!packet.WillCorrelationDataBytes.IsEmpty && !HasProperty(existingMask, PropertyIdentifier.CorrelationData))
      {
         len += 3 + (int)packet.WillCorrelationDataBytes.Length;
      }

      return len;
   }

   private static int CalculatePropertiesLength(ReadOnlySequence<byte> properties)
   {
      return PacketEncoder.GetVariableByteIntegerLength((int)properties.Length) + (int)properties.Length;
   }
}
