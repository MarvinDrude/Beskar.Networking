using Beskar.Mqtt.Common.Builders.Common;

namespace Beskar.Mqtt.Common.Builders.Unsubscribing;

public sealed class UnsubscribeOptionsBuilder(UnsubscribeOptions? options = null)
   : UserPropertiesBaseOptionsBuilder<UnsubscribeOptionsBuilder, UnsubscribeOptions>(options ?? new UnsubscribeOptions())
{
   /// <summary>
   /// Adds a topic to unsubcribe from.
   /// </summary>
   /// <param name="topic">The topic in question.</param>
   public UnsubscribeOptionsBuilder WithTopicFilter(ReadOnlySpan<char> topic)
   {
      _options.TopicFilters.Add(topic);
      return this;
   }

   /// <summary>
   /// Adds a topic to unsubcribe from.
   /// </summary>
   /// <param name="topic">The topic in question.</param>
   public UnsubscribeOptionsBuilder WithTopicFilter(string topic)
   {
      _options.TopicFilters.Add(topic);
      return this;
   }
}
