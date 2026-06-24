using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders.Version3;

public readonly ref partial struct PacketVersion3Encoder
{
   public void WriteUnsubscribe(UnsubscribeOptions options, ushort packetIdentifier)
   {
      var length = CalculateLength(options);
      var writer = new ByteWriter(_writer.GetSpan(length));

      try
      {
         var remainingLength = CalculateRemainingLength(options);
         PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Unsubscribe, 2, remainingLength);
         writer.WriteBigEndian(packetIdentifier);

         var enumerator = options.TopicFilters.GetEnumerator();
         while (enumerator.MoveNext())
         {
            var filter = enumerator.Current;
            writer.WriteBigEndian((ushort)filter.Length);
            if (filter.Length > 0)
            {
               writer.WriteBytes(filter);
            }
         }

         _writer.Advance(writer.Position);
      }
      finally
      {
         writer.Dispose();
      }
   }

   private int CalculateLength(UnsubscribeOptions options)
   {
      var remainingLength = CalculateRemainingLength(options);
      return PacketEncoder.CalculateFixedHeaderLength(remainingLength) + remainingLength;
   }

   private int CalculateRemainingLength(UnsubscribeOptions options)
   {
      var filtersLength = 0;
      var enumerator = options.TopicFilters.GetEnumerator();
      while (enumerator.MoveNext())
      {
         filtersLength += 2 + enumerator.Current.Length;
      }

      return 2 + filtersLength;
   }
}
