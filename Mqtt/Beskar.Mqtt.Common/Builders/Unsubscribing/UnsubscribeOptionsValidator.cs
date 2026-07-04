using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Validators.Common;

namespace Beskar.Mqtt.Common.Builders.Unsubscribing;

public static class UnsubscribeOptionsValidator
{
   public static VoidResult<StringError> Validate(UnsubscribeOptions options)
   {
      if (options.TopicFilters.Count > 0)
      {
         var enumerator = options.TopicFilters.GetEnumerator();
         while (enumerator.MoveNext())
         {
            var validateResult = TopicStringValidator.ValidateForSubscribe(enumerator.Current);
            if (validateResult.Failed)
            {
               return validateResult;
            }
         }
      }

      return true;
   }
}
