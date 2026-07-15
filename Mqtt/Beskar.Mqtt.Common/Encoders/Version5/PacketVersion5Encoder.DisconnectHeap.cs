using System.Buffers;
using System.Text;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteDisconnect(DisconnectOptions options)
   {
      var length = CalculateLength(options);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(options);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Disconnect, 0, remainingLength);

         if (remainingLength > 0)
         {
            writer.WriteByte((byte)options.ReasonCode);
            if (remainingLength > 1)
            {
               var propertiesLength = CalculatePropertiesLength(options);
               PacketEncoder.WriteVariableByteInteger(ref writer, (uint)propertiesLength);

               if (propertiesLength > 0)
               {
                  var propEncoder = writer.AsDisconnectPropertyEncoder();

                  try
                  {
                     if (options.SessionExpiryInterval.HasValue)
                     {
                        propEncoder.WriteSessionExpiryInterval(options.SessionExpiryInterval.Value);
                     }

                     if (options.ReasonString is { } reasonString)
                     {
                        var reasonStringBytes = Encoding.UTF8.GetBytes(reasonString);
                        propEncoder.WriteReasonString(reasonStringBytes);
                     }

                     if (options.ServerReference is { } serverReference)
                     {
                        var serverReferenceBytes = Encoding.UTF8.GetBytes(serverReference);
                        propEncoder.WriteServerReference(serverReferenceBytes);
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
            }
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private int CalculateLength(DisconnectOptions options)
   {
      var remainingLength = CalculateRemainingLength(options);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private int CalculateRemainingLength(DisconnectOptions options)
   {
      var propertiesLength = CalculatePropertiesLength(options);

      if (options.ReasonCode == DisconnectReasonCode.NormalDisconnection && propertiesLength == 0)
      {
         return 0;
      }

      if (propertiesLength == 0)
      {
         return 1;
      }

      return 1 + PacketEncoder.GetVariableByteIntegerLength(propertiesLength) + propertiesLength;
   }

   private static int CalculatePropertiesLength(DisconnectOptions options)
   {
      var len = 0;

      if (options.SessionExpiryInterval.HasValue)
      {
         len += 5;
      }

      if (options.ReasonString is { } reasonString)
      {
         len += 3 + Encoding.UTF8.GetByteCount(reasonString);
      }

      len += options.UserProperties.ByteCount;

      return len;
   }
}
