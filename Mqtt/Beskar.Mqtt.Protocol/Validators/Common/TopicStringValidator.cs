using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;

namespace Beskar.Mqtt.Protocol.Validators.Common;

public static class TopicStringValidator
{
   public static VoidResult<StringError> ValidateForSubscribe(ReadOnlySpan<byte> topicUtf8Bytes)
   {
      if (topicUtf8Bytes.IsEmpty)
      {
         return new StringError("Topic should not be empty.");
      }

      var indexOfHash = topicUtf8Bytes.IndexOf((byte)'#');

      if (indexOfHash >= 0 && indexOfHash < topicUtf8Bytes.Length - 1)
      {
         return new StringError("The character '#' is only allowed at the end of the topic.");
      }

      return true;
   }

   public static VoidResult<StringError> ValidateForSubscribe(ReadOnlySequence<byte> topicUtf8Bytes)
   {
      if (topicUtf8Bytes.IsEmpty)
      {
         return new StringError("Topic should not be empty.");
      }

      if (topicUtf8Bytes.IsSingleSegment)
      {
         return ValidateForSubscribe(topicUtf8Bytes.FirstSpan);
      }

      var position = topicUtf8Bytes.PositionOf((byte)'#');
      if (position is not null)
      {
         var sliceAfterHash = topicUtf8Bytes.Slice(position.Value);
         if (sliceAfterHash.Length > 1)
         {
            return new StringError("The character '#' is only allowed at the end of the topic.");
         }
      }

      return true;
   }

   public static VoidResult<StringError> Validate(ReadOnlySpan<byte> topicUtf8Bytes)
   {
      if (topicUtf8Bytes.IsEmpty)
      {
         return new StringError("Topic should not be empty.");
      }

      var index = topicUtf8Bytes.IndexOfAny((byte)'+', (byte)'#');
      if (index >= 0)
      {
         return new StringError("The characters '+' and '#' are not allowed in topics.");
      }

      return true;
   }
}
