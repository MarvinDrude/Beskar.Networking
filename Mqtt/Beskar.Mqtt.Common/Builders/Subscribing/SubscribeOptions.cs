using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Common.Interfaces.Builders;

namespace Beskar.Mqtt.Common.Builders.Subscribing;

/// <summary>
/// All options that are available for sending a SUBSCRIBE packet in MQTT.
/// </summary>
/// <param name="builderCapacity">What should the property builders start with as their byte capacity.</param>
public sealed class SubscribeOptions(int builderCapacity = -1) : UserPropertiesBaseOptions(builderCapacity)
{
   private readonly int _builderCapacity = builderCapacity;

   /// <summary>
   /// The subscription identifier.
   /// </summary>
   public uint SubscriptionIdentifier { get; set; }

   /// <summary>
   /// The topic filters that should be subscribed to.
   /// </summary>
   public TopicFilterListBuilder TopicFilters
      => field ??= new TopicFilterListBuilder(_builderCapacity == -1 ? 512 : _builderCapacity);

   /// <summary>
   /// Clears the property builders to 0 again. (Does not resize internal buffers)
   /// </summary>
   public override void Clear()
   {
      base.Clear();
      TopicFilters.Clear();
      SubscriptionIdentifier = 0;
   }

   public static SubscribeOptionsBuilder Create() => new();
}
