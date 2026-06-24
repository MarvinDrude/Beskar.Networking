using System.Buffers;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Encoders.Version3;

public readonly ref partial struct PacketVersion3Encoder
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
      return 2 + (int)options.TopicFilters.WrittenSequence.Length;
   }
}
