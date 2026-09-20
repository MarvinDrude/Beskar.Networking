using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Common.Serialization;
using Beskar.Mqtt.Protocol.Enums;

namespace Beskar.Mqtt.Common.Extensions;

/// <summary>
/// Extension methods providing high-level JSON streaming and subscription helpers for <see cref="IMqttClient"/>.
/// </summary>
public static class MqttClientStreamingExtensions
{
   /// <summary>
   /// Subscribes to an MQTT topic filter and returns an async stream of JSON-deserialized objects of type <typeparamref name="T"/>.
   /// </summary>
   public static IAsyncEnumerable<T> SubscribeJsonStream<T>(
      this IMqttClient client,
      string topicFilter,
      JsonSerializerOptions? jsonOptions = null,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      StreamSubscriptionOptions? options = null,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(client);
      return client.SubscribeStream(topicFilter, new JsonPayloadDecoder<T>(jsonOptions), qos, options, ct);
   }

   /// <summary>
   /// Subscribes to an MQTT topic filter and returns an async stream of JSON-deserialized objects of type <typeparamref name="T"/>
   /// using source-generated <see cref="JsonTypeInfo{T}"/>.
   /// </summary>
   public static IAsyncEnumerable<T> SubscribeJsonStream<T>(
      this IMqttClient client,
      string topicFilter,
      JsonTypeInfo<T> typeInfo,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      StreamSubscriptionOptions? options = null,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(client);
      ArgumentNullException.ThrowIfNull(typeInfo);
      return client.SubscribeStream(topicFilter, new JsonPayloadDecoder<T>(typeInfo), qos, options, ct);
   }

   /// <summary>
   /// Registers a topic-scoped callback that receives JSON-deserialized objects of type <typeparamref name="T"/>.
   /// Automatically re-subscribes on reconnect and unsubscribes when the returned token is disposed.
   /// </summary>
   public static Task<IAsyncDisposable> SubscribeJsonTopicAsync<T>(
      this IMqttClient client,
      string topicFilter,
      Func<T, MessageReceiveContext, CancellationToken, ValueTask> handler,
      JsonSerializerOptions? jsonOptions = null,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(client);
      return client.SubscribeTopicAsync(topicFilter, handler, new JsonPayloadDecoder<T>(jsonOptions), qos, ct);
   }

   /// <summary>
   /// Registers a topic-scoped callback that receives JSON-deserialized objects of type <typeparamref name="T"/>
   /// using source-generated <see cref="JsonTypeInfo{T}"/>.
   /// </summary>
   public static Task<IAsyncDisposable> SubscribeJsonTopicAsync<T>(
      this IMqttClient client,
      string topicFilter,
      Func<T, MessageReceiveContext, CancellationToken, ValueTask> handler,
      JsonTypeInfo<T> typeInfo,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(client);
      ArgumentNullException.ThrowIfNull(typeInfo);
      return client.SubscribeTopicAsync(topicFilter, handler, new JsonPayloadDecoder<T>(typeInfo), qos, ct);
   }

   /// <summary>
   /// Awaits the next JSON message matching the topic filter and optional predicate.
   /// </summary>
   public static Task<T> WaitForJsonMessageAsync<T>(
      this IMqttClient client,
      string topicFilter,
      Func<T, bool>? predicate = null,
      JsonSerializerOptions? jsonOptions = null,
      TimeSpan timeout = default,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(client);
      return client.WaitForMessageAsync(topicFilter, new JsonPayloadDecoder<T>(jsonOptions), predicate, timeout, ct);
   }

   /// <summary>
   /// Awaits the next JSON message matching the topic filter and optional predicate using source-generated <see cref="JsonTypeInfo{T}"/>.
   /// </summary>
   public static Task<T> WaitForJsonMessageAsync<T>(
      this IMqttClient client,
      string topicFilter,
      JsonTypeInfo<T> typeInfo,
      Func<T, bool>? predicate = null,
      TimeSpan timeout = default,
      CancellationToken ct = default)
   {
      ArgumentNullException.ThrowIfNull(client);
      ArgumentNullException.ThrowIfNull(typeInfo);
      return client.WaitForMessageAsync(topicFilter, new JsonPayloadDecoder<T>(typeInfo), predicate, timeout, ct);
   }
}
