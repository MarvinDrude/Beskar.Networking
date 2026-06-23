using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Common.Interfaces.Builders;

namespace Beskar.Mqtt.Common.Builders.Unsubscribing;

public sealed class UnsubscribeOptions(int builderCapacity = -1) : IClearableOptions
{
   public Utf8StringListBuilder TopicFilters
      => field ??= new Utf8StringListBuilder(builderCapacity == -1 ? 512 :  builderCapacity);

   public UserPropertyListBuilder UserProperties
      => field ??= new UserPropertyListBuilder(builderCapacity == -1 ? 128 :  builderCapacity);

   public void Clear()
   {
      TopicFilters.Clear();
      UserProperties.Clear();
   }
}
