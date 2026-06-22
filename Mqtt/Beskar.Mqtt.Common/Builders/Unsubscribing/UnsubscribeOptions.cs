using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Common.Interfaces.Builders;

namespace Beskar.Mqtt.Common.Builders.Unsubscribing;

public sealed class UnsubscribeOptions : IClearableOptions
{
   public Utf8StringListBuilder TopicFilters => field ??= new Utf8StringListBuilder(512);



   public void Clear()
   {

   }
}
