using System.Buffers;
using System.Text;
using Beskar.Mqtt.Common.Builders.Common;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Builders.Publishing;

public sealed class PublishOptionsBuilder(PublishOptions? options = null)
   : UserPropertiesBaseOptionsBuilder<PublishOptionsBuilder, PublishOptions>(options ?? new PublishOptions())
{
   public PublishOptionsBuilder WithDup(bool dup = true)
   {
      _options.Dup = dup;
      return this;
   }

   public PublishOptionsBuilder WithQualityOfService(QualityOfServiceType qos)
   {
      _options.QualityOfService = qos;
      return this;
   }

   public PublishOptionsBuilder WithRetain(bool retain = true)
   {
      _options.Retain = retain;
      return this;
   }

   public PublishOptionsBuilder WithTopic(string topic)
   {
      _options.TopicUtf8Bytes = Encoding.UTF8.GetBytes(topic);
      return this;
   }

   public PublishOptionsBuilder WithTopic(ReadOnlySpan<char> topic)
   {
      _options.TopicUtf8Bytes = Encoding.UTF8.GetBytes([.. topic]);
      return this;
   }

   public PublishOptionsBuilder WithTopic(ReadOnlySpan<byte> topicUtf8Bytes)
   {
      _options.TopicUtf8Bytes = topicUtf8Bytes.ToArray();
      return this;
   }

   public PublishOptionsBuilder WithPayload(ReadOnlyMemory<byte> payload)
   {
      _options.Payload = new ReadOnlySequence<byte>(payload);
      return this;
   }

   public PublishOptionsBuilder WithPayload(ReadOnlySequence<byte> payload)
   {
      _options.Payload = payload;
      return this;
   }

   public PublishOptionsBuilder WithPayload(byte[] payload)
   {
      _options.Payload = new ReadOnlySequence<byte>(payload);
      return this;
   }

   public PublishOptionsBuilder WithPayload(string payload)
   {
      _options.Payload = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(payload));
      return this;
   }

   /// <summary>
   /// Sets the Payload Format Indicator.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PublishOptionsBuilder WithPayloadFormat(PayloadFormat format)
   {
      _options.PayloadFormat = format;
      return this;
   }

   /// <summary>
   /// Sets the Message Expiry Interval.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PublishOptionsBuilder WithMessageExpiryInterval(uint interval)
   {
      _options.MessageExpiryInterval = interval;
      return this;
   }

   /// <summary>
   /// Sets the Topic Alias.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PublishOptionsBuilder WithTopicAlias(ushort alias)
   {
      _options.TopicAlias = alias;
      return this;
   }

   /// <summary>
   /// Sets the Response Topic.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PublishOptionsBuilder WithResponseTopic(string responseTopic)
   {
      _options.ResponseTopicUtf8Bytes = Encoding.UTF8.GetBytes(responseTopic);
      return this;
   }

   /// <summary>
   /// Sets the Response Topic.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PublishOptionsBuilder WithResponseTopic(ReadOnlySpan<char> responseTopic)
   {
      _options.ResponseTopicUtf8Bytes = Encoding.UTF8.GetBytes([.. responseTopic]);
      return this;
   }

   /// <summary>
   /// Sets the Response Topic.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PublishOptionsBuilder WithResponseTopic(ReadOnlySpan<byte> responseTopicUtf8Bytes)
   {
      _options.ResponseTopicUtf8Bytes = responseTopicUtf8Bytes.ToArray();
      return this;
   }

   /// <summary>
   /// Sets the Correlation Data.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PublishOptionsBuilder WithCorrelationData(ReadOnlyMemory<byte> correlationData)
   {
      _options.CorrelationData = correlationData;
      return this;
   }

   /// <summary>
   /// Sets the Correlation Data.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PublishOptionsBuilder WithCorrelationData(byte[] correlationData)
   {
      _options.CorrelationData = correlationData;
      return this;
   }

   /// <summary>
   /// Sets the Content Type.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PublishOptionsBuilder WithContentType(string contentType)
   {
      _options.ContentTypeUtf8Bytes = Encoding.UTF8.GetBytes(contentType);
      return this;
   }

   /// <summary>
   /// Sets the Content Type.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PublishOptionsBuilder WithContentType(ReadOnlySpan<char> contentType)
   {
      _options.ContentTypeUtf8Bytes = Encoding.UTF8.GetBytes([.. contentType]);
      return this;
   }

   /// <summary>
   /// Sets the Content Type.
   /// <remarks>MQTT 5.0.0 and above required.</remarks>
   /// </summary>
   public PublishOptionsBuilder WithContentType(ReadOnlySpan<byte> contentTypeUtf8Bytes)
   {
      _options.ContentTypeUtf8Bytes = contentTypeUtf8Bytes.ToArray();
      return this;
   }
}
