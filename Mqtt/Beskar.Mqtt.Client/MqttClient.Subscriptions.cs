using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Common.Serialization;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient
{
   public IAsyncEnumerable<MqttPublishMessage> SubscribeStream(
      string topicFilter,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      StreamSubscriptionOptions? options = null,
      CancellationToken ct = default)
   {
      return _subscriptionManager.SubscribeStream(topicFilter, qos, options, ct);
   }

   public IAsyncEnumerable<T> SubscribeStream<T>(
      string topicFilter,
      Func<ReadOnlyMemory<byte>, T> decoder,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      StreamSubscriptionOptions? options = null,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(decoder);
      return _subscriptionManager.SubscribeStream(topicFilter, new DelegatePayloadDecoder<T>(decoder), qos, options, ct);
   }

   public IAsyncEnumerable<T> SubscribeStream<T>(
      string topicFilter,
      IMqttPayloadDecoder<T> decoder,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      StreamSubscriptionOptions? options = null,
      CancellationToken ct = default)
   {
      return _subscriptionManager.SubscribeStream(topicFilter, decoder, qos, options, ct);
   }

   public Task<IAsyncDisposable> SubscribeTopicAsync<T>(
      string topicFilter,
      Func<T, MessageReceiveContext, CancellationToken, ValueTask> handler,
      Func<ReadOnlyMemory<byte>, T> decoder,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(decoder);
      return _subscriptionManager.SubscribeTopicAsync(topicFilter, handler, new DelegatePayloadDecoder<T>(decoder), qos, ct);
   }

   public Task<IAsyncDisposable> SubscribeTopicAsync<T>(
      string topicFilter,
      Func<T, MessageReceiveContext, CancellationToken, ValueTask> handler,
      IMqttPayloadDecoder<T> decoder,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      CancellationToken ct = default)
   {
      return _subscriptionManager.SubscribeTopicAsync(topicFilter, handler, decoder, qos, ct);
   }

   public Task<IAsyncDisposable> SubscribeTopicAsync(
      string topicFilter,
      Func<MessageReceiveContext, CancellationToken, ValueTask> handler,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      CancellationToken ct = default)
   {
      return _subscriptionManager.SubscribeTopicAsync(topicFilter, handler, qos, ct);
   }

   public Task<T> WaitForMessageAsync<T>(
      string topicFilter,
      Func<ReadOnlyMemory<byte>, T> decoder,
      Func<T, bool>? predicate = null,
      TimeSpan timeout = default,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(decoder);
      return _subscriptionManager.WaitForMessageAsync(topicFilter, new DelegatePayloadDecoder<T>(decoder), predicate, timeout, ct);
   }

   public Task<T> WaitForMessageAsync<T>(
      string topicFilter,
      IMqttPayloadDecoder<T> decoder,
      Func<T, bool>? predicate = null,
      TimeSpan timeout = default,
      CancellationToken ct = default)
   {
      return _subscriptionManager.WaitForMessageAsync(topicFilter, decoder, predicate, timeout, ct);
   }
}
