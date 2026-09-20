namespace Beskar.Mqtt.Common.Matching;

/// <summary>
/// High-performance zero-allocation matcher for MQTT topic filters and concrete topics,
/// strictly adhering to MQTT 3.1.1 and MQTT 5.0 wildcard matching specifications ('+' and '#').
/// </summary>
public static class MqttTopicMatcher
{
   /// <summary>
   /// Checks if a concrete MQTT topic matches the specified MQTT topic filter.
   /// </summary>
   /// <param name="topicFilter">The topic filter (can contain '+' and '#').</param>
   /// <param name="topic">The concrete topic (cannot contain wildcards).</param>
   /// <returns><c>true</c> if the topic matches the filter; otherwise, <c>false</c>.</returns>
   public static bool IsMatch(ReadOnlySpan<char> topicFilter, ReadOnlySpan<char> topic)
   {
      if (topicFilter.IsEmpty || topic.IsEmpty)
         return false;

      // Leading $ special rule:
      // Wildcard '+' or '#' at the first level does NOT match a leading '$' in topic.
      if (topic[0] == '$' && (topicFilter[0] == '+' || topicFilter[0] == '#'))
      {
         return false;
      }

      if (topicFilter.IndexOfAny('+', '#') == -1)
      {
         return topicFilter.SequenceEqual(topic);
      }

      var filterRest = topicFilter;
      var topicRest = topic;

      while (true)
      {
         var filterSlash = filterRest.IndexOf('/');
         ReadOnlySpan<char> filterLevel;
         if (filterSlash >= 0)
         {
            filterLevel = filterRest[..filterSlash];
            filterRest = filterRest[(filterSlash + 1)..];
         }
         else
         {
            filterLevel = filterRest;
            filterRest = default;
         }

         if (filterLevel is "#")
         {
            return true;
         }

         var topicSlash = topicRest.IndexOf('/');
         ReadOnlySpan<char> topicLevel;
         var topicHasMore = false;
         if (topicSlash >= 0)
         {
            topicLevel = topicRest[..topicSlash];
            topicRest = topicRest[(topicSlash + 1)..];
            topicHasMore = true;
         }
         else
         {
            topicLevel = topicRest;
            topicRest = default;
            topicHasMore = false;
         }

         if (filterLevel is not "+" && !filterLevel.SequenceEqual(topicLevel))
         {
            return false;
         }

         if (filterSlash < 0)
         {
            return !topicHasMore && topicRest.IsEmpty;
         }

         if (!topicHasMore && topicRest.IsEmpty)
         {
            return filterRest is "#";
         }
      }
   }

   /// <summary>
   /// UTF-8 byte span overload of <see cref="IsMatch(ReadOnlySpan{char}, ReadOnlySpan{char})"/>.
   /// </summary>
   public static bool IsMatch(ReadOnlySpan<byte> topicFilter, ReadOnlySpan<byte> topic)
   {
      if (topicFilter.IsEmpty || topic.IsEmpty)
         return false;

      const byte dollar = (byte)'$';
      const byte plus = (byte)'+';
      const byte hash = (byte)'#';
      const byte slash = (byte)'/';

      if (topic[0] == dollar && (topicFilter[0] == plus || topicFilter[0] == hash))
      {
         return false;
      }

      if (topicFilter.IndexOfAny(plus, hash) == -1)
      {
         return topicFilter.SequenceEqual(topic);
      }

      var filterRest = topicFilter;
      var topicRest = topic;

      while (true)
      {
         var filterSlash = filterRest.IndexOf(slash);
         ReadOnlySpan<byte> filterLevel;
         if (filterSlash >= 0)
         {
            filterLevel = filterRest[..filterSlash];
            filterRest = filterRest[(filterSlash + 1)..];
         }
         else
         {
            filterLevel = filterRest;
            filterRest = default;
         }

         if (filterLevel.Length == 1 && filterLevel[0] == hash)
         {
            return true;
         }

         var topicSlash = topicRest.IndexOf(slash);
         ReadOnlySpan<byte> topicLevel;
         var topicHasMore = false;
         if (topicSlash >= 0)
         {
            topicLevel = topicRest[..topicSlash];
            topicRest = topicRest[(topicSlash + 1)..];
            topicHasMore = true;
         }
         else
         {
            topicLevel = topicRest;
            topicRest = default;
            topicHasMore = false;
         }

         var isPlus = filterLevel.Length == 1 && filterLevel[0] == plus;
         if (!isPlus && !filterLevel.SequenceEqual(topicLevel))
         {
            return false;
         }

         if (filterSlash < 0)
         {
            return !topicHasMore && topicRest.IsEmpty;
         }

         if (!topicHasMore && topicRest.IsEmpty)
         {
            return filterRest.Length == 1 && filterRest[0] == hash;
         }
      }
   }
}
