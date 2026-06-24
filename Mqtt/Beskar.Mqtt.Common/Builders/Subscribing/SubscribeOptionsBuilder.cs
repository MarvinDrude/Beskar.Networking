using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Builders.Subscribing;

public sealed class SubscribeOptionsBuilder(SubscribeOptions? options = null)
   : UserPropertiesBaseOptionsBuilder<SubscribeOptionsBuilder, SubscribeOptions>(options ?? new SubscribeOptions())
{
   /// <summary>
   /// Sets the subscription identifier.
   /// </summary>
   public SubscribeOptionsBuilder WithSubscriptionIdentifier(uint subscriptionIdentifier)
   {
      _options.SubscriptionIdentifier = subscriptionIdentifier;
      return this;
   }

   /// <summary>
   /// Adds a topic filter to subscribe to.
   /// </summary>
   public SubscribeOptionsBuilder WithTopicFilter(
      string topic,
      QualityOfServiceType qos,
      bool noLocal = false,
      bool retainAsPublished = false,
      RetainHandlingType retainHandling = RetainHandlingType.SendAtSubscription)
   {
      _options.TopicFilters.Add(topic, qos, noLocal, retainAsPublished, retainHandling);
      return this;
   }

   /// <summary>
   /// Adds a topic filter to subscribe to.
   /// </summary>
   public SubscribeOptionsBuilder WithTopicFilter(
      ReadOnlySpan<char> topic,
      QualityOfServiceType qos,
      bool noLocal = false,
      bool retainAsPublished = false,
      RetainHandlingType retainHandling = RetainHandlingType.SendAtSubscription)
   {
      _options.TopicFilters.Add(topic, qos, noLocal, retainAsPublished, retainHandling);
      return this;
   }

   /// <summary>
   /// Adds a topic filter to subscribe to.
   /// </summary>
   public SubscribeOptionsBuilder WithTopicFilter(
      ReadOnlySpan<byte> topicUtf8Bytes,
      QualityOfServiceType qos,
      bool noLocal = false,
      bool retainAsPublished = false,
      RetainHandlingType retainHandling = RetainHandlingType.SendAtSubscription)
   {
      _options.TopicFilters.Add(topicUtf8Bytes, qos, noLocal, retainAsPublished, retainHandling);
      return this;
   }
}
