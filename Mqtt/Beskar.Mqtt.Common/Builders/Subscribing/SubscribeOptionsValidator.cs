using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Validators.Common;

namespace Beskar.Mqtt.Common.Builders.Subscribing;

public static class SubscribeOptionsValidator
{
   public static VoidResult<StringError> Validate(SubscribeOptions options)
   {
      if (options.TopicFilters.Count > 0)
      {
         var enumerator = options.TopicFilters.GetEnumerator();
         while (enumerator.MoveNext())
         {
            var validateResult = TopicStringValidator.ValidateForSubscribe(enumerator.Current.TopicUtf8Bytes);
            if (validateResult.Failed)
            {
               return validateResult;
            }
         }
      }

      return true;
   }
}
