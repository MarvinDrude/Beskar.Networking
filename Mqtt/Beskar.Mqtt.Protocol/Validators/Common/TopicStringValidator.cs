using System.Buffers;
using System.Text.Unicode;
using Beskar.Memory.Owners;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;

namespace Beskar.Mqtt.Protocol.Validators.Common;

public static class TopicStringValidator
{
   private static VoidResult<StringError> ValidateUtf8AndNoNull(ReadOnlySpan<byte> topicUtf8Bytes)
   {
      if (topicUtf8Bytes.IsEmpty)
      {
         return new StringError("Topic should not be empty.");
      }

      if (topicUtf8Bytes.Contains((byte)0))
      {
         return new StringError("Topic must not contain null character (U+0000).");
      }

      if (!Utf8.IsValid(topicUtf8Bytes))
      {
         return new StringError("Topic must be valid UTF-8.");
      }

      return true;
   }

   public static VoidResult<StringError> ValidateForSubscribe(ReadOnlySpan<byte> topicUtf8Bytes)
   {
      var baseValidation = ValidateUtf8AndNoNull(topicUtf8Bytes);
      if (baseValidation.Failed)
      {
         return baseValidation.Error;
      }

      var indexOfHash = topicUtf8Bytes.IndexOf((byte)'#');
      if (indexOfHash >= 0)
      {
         if (indexOfHash < topicUtf8Bytes.Length - 1)
         {
            return new StringError("The character '#' is only allowed at the end of the topic.");
         }

         if (indexOfHash > 0 && topicUtf8Bytes[indexOfHash - 1] != (byte)'/')
         {
            return new StringError("The character '#' must follow a '/' separator or be the entire filter.");
         }
      }

      for (var i = 0; i < topicUtf8Bytes.Length; i++)
      {
         if (topicUtf8Bytes[i] == (byte)'+')
         {
            if ((i > 0 && topicUtf8Bytes[i - 1] != (byte)'/') ||
                (i + 1 < topicUtf8Bytes.Length && topicUtf8Bytes[i + 1] != (byte)'/'))
            {
               return new StringError("The character '+' must occupy an entire topic level.");
            }
         }
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

      if (topicUtf8Bytes.Length > ushort.MaxValue)
      {
         return new StringError("Topic exceeds maximum UTF-8 string length.");
      }

      var length = (int)topicUtf8Bytes.Length;
      using var owner = length <= 256
         ? new SpanOwner<byte>(stackalloc byte[length])
         : new SpanOwner<byte>(length);
      var span = owner.Span;
      topicUtf8Bytes.CopyTo(span);

      return ValidateForSubscribe(span);
   }

   public static VoidResult<StringError> Validate(ReadOnlySpan<byte> topicUtf8Bytes)
   {
      var baseValidation = ValidateUtf8AndNoNull(topicUtf8Bytes);
      if (baseValidation.Failed)
      {
         return baseValidation.Error;
      }

      var index = topicUtf8Bytes.IndexOfAny((byte)'+', (byte)'#');
      if (index >= 0)
      {
         return new StringError("The characters '+' and '#' are not allowed in topics.");
      }

      return true;
   }
}
