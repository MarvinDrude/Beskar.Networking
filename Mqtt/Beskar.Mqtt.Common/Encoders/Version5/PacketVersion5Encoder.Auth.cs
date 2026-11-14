using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Common.Options;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteAuth(in AuthPacket packet)
   {
      var existingMask = CollectPresentProperties(packet.PropertiesBytes);
      var propertiesLength = CalculatePropertiesLength(in packet, existingMask);
      var remainingLength = CalculateRemainingLength(in packet, propertiesLength);
      var length = PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Auth, 0, remainingLength);

         if (remainingLength > 0)
         {
            writer.WriteByte((byte)packet.ReasonCode);
            PacketEncoder.WriteVariableByteInteger(ref writer, (uint)propertiesLength);

            if (propertiesLength > 0)
            {
               var propEncoder = writer.AsAuthPropertyEncoder();
               try
               {
                  if (!packet.AuthenticationMethodUtf8Bytes.IsEmpty
                      && !HasProperty(existingMask, PropertyIdentifier.AuthenticationMethod))
                  {
                     propEncoder.Encoder.Write(PropertyIdentifier.AuthenticationMethod, packet.AuthenticationMethodUtf8Bytes);
                  }
                  if (!packet.AuthenticationDataBytes.IsEmpty
                      && !HasProperty(existingMask, PropertyIdentifier.AuthenticationData))
                  {
                     propEncoder.Encoder.Write(PropertyIdentifier.AuthenticationData, packet.AuthenticationDataBytes);
                  }
                  if (!packet.ReasonUtf8Bytes.IsEmpty
                      && !HasProperty(existingMask, PropertyIdentifier.ReasonString))
                  {
                     propEncoder.Encoder.Write(PropertyIdentifier.ReasonString, packet.ReasonUtf8Bytes);
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

   public void WriteAuth(AuthPacketOptions options)
   {
      var length = CalculateLength(options);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(options);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Auth, 0, remainingLength);

         if (remainingLength > 0)
         {
            writer.WriteByte((byte)options.ReasonCode);
            
            var propertiesLength = CalculatePropertiesLength(options);
            PacketEncoder.WriteVariableByteInteger(ref writer, (uint)propertiesLength);
            
            if (propertiesLength > 0)
            {
               var propEncoder = writer.AsAuthPropertyEncoder();
               try
               {
                  if (!options.AuthenticationMethodUtf8Bytes.IsEmpty)
                  {
                     propEncoder.WriteAuthenticationMethod(options.AuthenticationMethodUtf8Bytes.Span);
                  }
                  if (!options.AuthenticationDataBytes.IsEmpty)
                  {
                     propEncoder.WriteAuthenticationData(options.AuthenticationDataBytes.Span);
                  }
                  if (!options.ReasonUtf8Bytes.IsEmpty)
                  {
                     propEncoder.WriteReasonString(options.ReasonUtf8Bytes.Span);
                  }
               }
               finally
               {
                  writer = propEncoder.Encoder.Writer;
               }

               if (!options.PropertiesBytes.IsEmpty)
               {
                  writer.WriteBytes(options.PropertiesBytes.Span);
               }
            }
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private static int CalculateLength(AuthPacketOptions options)
   {
      var remainingLength = CalculateRemainingLength(options);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private static int CalculateRemainingLength(AuthPacketOptions options)
   {
      if (options.ReasonCode == AuthenticateReasonCode.Success 
          && options.AuthenticationMethodUtf8Bytes.IsEmpty 
          && options.AuthenticationDataBytes.IsEmpty 
          && options.ReasonUtf8Bytes.IsEmpty 
          && options.PropertiesBytes.IsEmpty)
      {
         return 0;
      }

      var propertiesLength = CalculatePropertiesLength(options);
      return 1 + PacketEncoder.GetVariableByteIntegerLength(propertiesLength) + propertiesLength;
   }

   private static int CalculatePropertiesLength(AuthPacketOptions options)
   {
      var len = 0;
      if (!options.AuthenticationMethodUtf8Bytes.IsEmpty)
      {
         len += 3 + options.AuthenticationMethodUtf8Bytes.Length;
      }
      if (!options.AuthenticationDataBytes.IsEmpty)
      {
         len += 3 + options.AuthenticationDataBytes.Length;
      }
      if (!options.ReasonUtf8Bytes.IsEmpty)
      {
         len += 3 + options.ReasonUtf8Bytes.Length;
      }
      len += options.PropertiesBytes.Length;
      return len;
   }

   private static int CalculateRemainingLength(in AuthPacket packet, int propertiesLength)
   {
      if (packet.ReasonCode == AuthenticateReasonCode.Success && propertiesLength == 0)
      {
         return 0;
      }

      return 1 + PacketEncoder.GetVariableByteIntegerLength(propertiesLength) + propertiesLength;
   }

   private static int CalculatePropertiesLength(in AuthPacket packet, ulong existingMask)
   {
      var len = 0;
      if (!packet.AuthenticationMethodUtf8Bytes.IsEmpty
          && !HasProperty(existingMask, PropertyIdentifier.AuthenticationMethod))
      {
         len += 3 + (int)packet.AuthenticationMethodUtf8Bytes.Length;
      }
      if (!packet.AuthenticationDataBytes.IsEmpty
          && !HasProperty(existingMask, PropertyIdentifier.AuthenticationData))
      {
         len += 3 + (int)packet.AuthenticationDataBytes.Length;
      }
      if (!packet.ReasonUtf8Bytes.IsEmpty
          && !HasProperty(existingMask, PropertyIdentifier.ReasonString))
      {
         len += 3 + (int)packet.ReasonUtf8Bytes.Length;
      }
      len += (int)packet.PropertiesBytes.Length;
      return len;
   }
}
