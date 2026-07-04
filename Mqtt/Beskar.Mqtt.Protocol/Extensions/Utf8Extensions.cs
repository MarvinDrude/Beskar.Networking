using System.Buffers;
using System.Text;
using Beskar.Memory.Owners;

namespace Beskar.Mqtt.Protocol.Extensions;

public static class Utf8Extensions
{
   extension(ReadOnlySequence<byte> data)
   {
      public string? GetUtf8String()
      {
         if (data.IsEmpty) return null;

         if (data.IsSingleSegment)
         {
            return Encoding.UTF8.GetString(data.FirstSpan);
         }

         var length = (int)data.Length;
         using var owner = length <= 256
            ? new SpanOwner<byte>(stackalloc byte[length])
            : new SpanOwner<byte>(length);
         var span = owner.Span;

         data.CopyTo(span);
         return Encoding.UTF8.GetString(span);
      }
   }

   extension(ReadOnlyMemory<byte> data)
   {
      public string? GetUtf8String()
      {
         return data.IsEmpty ? null : Encoding.UTF8.GetString(data.Span);
      }
   }
}
