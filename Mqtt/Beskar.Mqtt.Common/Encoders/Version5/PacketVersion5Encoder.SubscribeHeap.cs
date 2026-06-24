using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders.Version5;

public readonly ref partial struct PacketVersion5Encoder
{
   public void WriteSubscribe(SubscribeOptions options, ushort packetIdentifier)
   {
      var length = CalculateLength(options);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(options);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Subscribe, 2, remainingLength);
         writer.WriteBigEndian(packetIdentifier);

         var propertiesLength = CalculatePropertiesLength(options);
         PacketEncoder.WriteVariableByteInteger(ref writer, (uint)propertiesLength);

         if (propertiesLength > 0)
         {
            var propEncoder = writer.AsSubscribePropertyEncoder();

            if (options.SubscriptionIdentifier > 0)
            {
               propEncoder.WriteSubscriptionIdentifier(options.SubscriptionIdentifier);
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

            writer = propEncoder.Encoder.Writer;
         }

         var filtersSequence = options.TopicFilters.WrittenSequence;
         if (filtersSequence.IsSingleSegment)
         {
            writer.WriteBytes(filtersSequence.First.Span);
         }
         else
         {
            foreach (var memory in filtersSequence)
            {
               writer.WriteBytes(memory.Span);
            }
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private int CalculateLength(SubscribeOptions options)
   {
      var remainingLength = CalculateRemainingLength(options);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private int CalculateRemainingLength(SubscribeOptions options)
   {
      var propertiesLength = CalculatePropertiesLength(options);
      return 2 + PacketEncoder.GetVariableByteIntegerLength(propertiesLength) + propertiesLength + (int)options.TopicFilters.WrittenSequence.Length;
   }

   private static int CalculatePropertiesLength(SubscribeOptions options)
   {
      var len = 0;

      if (options.SubscriptionIdentifier > 0)
      {
         len += 1 + PacketEncoder.GetVariableByteIntegerLength((int)options.SubscriptionIdentifier);
      }

      len += options.UserProperties.ByteCount;

      return len;
   }
}
