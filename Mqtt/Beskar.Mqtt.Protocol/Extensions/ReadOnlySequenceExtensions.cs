using System.Buffers;

namespace Beskar.Mqtt.Protocol.Extensions;

public static class ReadOnlySequenceExtensions
{
   extension<T>(ReadOnlySequence<T> sequence)
   {
      public ReadOnlyMemory<T>? ToNullableMemory()
      {
         return sequence.IsEmpty ? null : sequence.ToArray();
      }
   }
}
