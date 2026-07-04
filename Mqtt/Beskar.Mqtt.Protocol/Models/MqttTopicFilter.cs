using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;

namespace Beskar.Mqtt.Protocol.Models;

/// <summary>
/// Heap class version of a topic filter
/// </summary>
public class MqttTopicFilter(in TopicFilter topicFilter)
{
   public bool RetainAsPublished { get; init; } = topicFilter.RetainAsPublished;

   public RetainHandlingType RetainHandling { get; init; } = topicFilter.RetainHandling;

   public bool NoLocal { get; init; } = topicFilter.NoLocal;

   public QualityOfServiceType QualityOfService { get; init; } = topicFilter.QualityOfService;

   public string Topic { get; init; } = topicFilter.TopicUtf8Bytes.GetUtf8String() ?? string.Empty;
}
