using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Common.Interfaces.Builders;

namespace Beskar.Mqtt.Common.Builders.Unsubscribing;

/// <summary>
/// All options that are avilable for sending an UNSUBSCRIBE packet in MQTT.
/// </summary>
/// <param name="builderCapacity">What should the property builders start with as their byte capacity.</param>
public sealed class UnsubscribeOptions(int builderCapacity = -1) : UserPropertiesBaseOptions(builderCapacity)
{
   private readonly int _builderCapacity = builderCapacity;

   /// <summary>
   /// The topic filters that should be unsubscribed from.
   /// </summary>
   public Utf8StringListBuilder TopicFilters
      => field ??= new Utf8StringListBuilder(_builderCapacity == -1 ? 512 :  _builderCapacity);

   /// <summary>
   /// Clears the property builders to 0 again. (Does not resize internal buffers)
   /// </summary>
   public override void Clear()
   {
      base.Clear();
      TopicFilters.Clear();
   }
}
