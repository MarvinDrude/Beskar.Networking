using System.Collections.Concurrent;
using System.Threading.Channels;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Common.Matching;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Common.Serialization;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Client.Internal;

/// <summary>
/// Manages active MQTT subscriptions, client-side topic demultiplexing,
/// automatic re-subscription across reconnects, and modern async streaming.
/// </summary>
internal sealed class MqttSubscriptionManager : IAsyncDisposable
{
   private readonly MqttClient _client;
   private readonly ConcurrentDictionary<string, TopicSubscriptionEntry> _subscriptions = new();

   private readonly IDisposable _receiveHandlerToken;
   private readonly IDisposable _connectedHandlerToken;

   private int _disposed;

   public MqttSubscriptionManager(MqttClient client)
   {
      _client = client ?? throw new ArgumentNullException(nameof(client));

      _receiveHandlerToken = _client.AddMessageReceiveHandler(DispatchMessageAsync);
      _connectedHandlerToken = _client.AddConnectedHandler(OnClientConnectedAsync);
   }

   public IAsyncEnumerable<MqttPublishMessage> SubscribeStream(
      string topicFilter,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      StreamSubscriptionOptions? options = null,
      CancellationToken ct = default)
   {
      ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
      ArgumentException.ThrowIfNullOrWhiteSpace(topicFilter);

      var effectiveOptions = options ?? StreamSubscriptionOptions.Default;
      var channel = Channel.CreateBounded<MqttPublishMessage>(new BoundedChannelOptions(effectiveOptions.BoundedCapacity)
      {
         FullMode = effectiveOptions.FullMode,
         SingleReader = true,
         SingleWriter = false
      });

      var sink = new RawChannelSubscriptionSink(channel.Writer, effectiveOptions.FullMode);
      var entry = GetOrAddEntry(topicFilter);
      var isFirst = entry.AddSink(sink, qos);

      if (isFirst && _client.IsConnected)
      {
         _ = TrySubscribeBrokerAsync(topicFilter, qos, ct);
      }

      return new MqttMessageStream<MqttPublishMessage>(channel, () => RemoveSinkAsync(topicFilter, sink));
   }

   public IAsyncEnumerable<T> SubscribeStream<T>(
      string topicFilter,
      IMqttPayloadDecoder<T> decoder,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      StreamSubscriptionOptions? options = null,
      CancellationToken ct = default)
   {
      ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
      ArgumentException.ThrowIfNullOrWhiteSpace(topicFilter);
      ArgumentNullException.ThrowIfNull(decoder);

      var effectiveOptions = options ?? StreamSubscriptionOptions.Default;
      var channel = Channel.CreateBounded<T>(new BoundedChannelOptions(effectiveOptions.BoundedCapacity)
      {
         FullMode = effectiveOptions.FullMode,
         SingleReader = true,
         SingleWriter = false
      });

      var sink = new ChannelSubscriptionSink<T>(channel.Writer, decoder, effectiveOptions.FullMode);
      var entry = GetOrAddEntry(topicFilter);
      var isFirst = entry.AddSink(sink, qos);

      if (isFirst && _client.IsConnected)
      {
         _ = TrySubscribeBrokerAsync(topicFilter, qos, ct);
      }

      return new MqttMessageStream<T>(channel, () => RemoveSinkAsync(topicFilter, sink));
   }

   public async Task<IAsyncDisposable> SubscribeTopicAsync<T>(
      string topicFilter,
      Func<T, MessageReceiveContext, CancellationToken, ValueTask> handler,
      IMqttPayloadDecoder<T> decoder,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      CancellationToken ct = default)
   {
      ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
      ArgumentException.ThrowIfNullOrWhiteSpace(topicFilter);
      ArgumentNullException.ThrowIfNull(handler);
      ArgumentNullException.ThrowIfNull(decoder);

      var sink = new CallbackSubscriptionSink<T>(handler, decoder);
      var entry = GetOrAddEntry(topicFilter);
      var isFirst = entry.AddSink(sink, qos);

      if (isFirst && _client.IsConnected)
      {
         await TrySubscribeBrokerAsync(topicFilter, qos, ct).ConfigureAwait(false);
      }

      return new SubscriptionDisposable(() => RemoveSinkAsync(topicFilter, sink));
   }

   public async Task<IAsyncDisposable> SubscribeTopicAsync(
      string topicFilter,
      Func<MessageReceiveContext, CancellationToken, ValueTask> handler,
      QualityOfServiceType qos = QualityOfServiceType.AtLeastOnce,
      CancellationToken ct = default)
   {
      ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
      ArgumentException.ThrowIfNullOrWhiteSpace(topicFilter);
      ArgumentNullException.ThrowIfNull(handler);

      var sink = new RawCallbackSubscriptionSink(handler);
      var entry = GetOrAddEntry(topicFilter);
      var isFirst = entry.AddSink(sink, qos);

      if (isFirst && _client.IsConnected)
      {
         await TrySubscribeBrokerAsync(topicFilter, qos, ct).ConfigureAwait(false);
      }

      return new SubscriptionDisposable(() => RemoveSinkAsync(topicFilter, sink));
   }

   public async Task<T> WaitForMessageAsync<T>(
      string topicFilter,
      IMqttPayloadDecoder<T> decoder,
      Func<T, bool>? predicate = null,
      TimeSpan timeout = default,
      CancellationToken ct = default)
   {
      ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);
      ArgumentException.ThrowIfNullOrWhiteSpace(topicFilter);
      ArgumentNullException.ThrowIfNull(decoder);

      using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      if (timeout > TimeSpan.Zero && timeout != Timeout.InfiniteTimeSpan)
      {
         linkedCts.CancelAfter(timeout);
      }

      var tcs = new TaskCompletionSource<T>(TaskCreationOptions.RunContinuationsAsynchronously);

      var registration = await SubscribeTopicAsync<T>(
         topicFilter,
         (item, _, _) =>
         {
            if (predicate == null || predicate(item))
            {
               tcs.TrySetResult(item);
            }
            return ValueTask.CompletedTask;
         },
         decoder,
         QualityOfServiceType.AtLeastOnce,
         linkedCts.Token).ConfigureAwait(false);

      await using (registration.ConfigureAwait(false))
      {
         try
         {
            return await tcs.Task.WaitAsync(linkedCts.Token).ConfigureAwait(false);
         }
         catch (OperationCanceledException) when (!ct.IsCancellationRequested && linkedCts.IsCancellationRequested)
         {
            throw new TimeoutException($"Timed out after {timeout.TotalMilliseconds}ms waiting for message on topic '{topicFilter}'.");
         }
      }
   }

   private TopicSubscriptionEntry GetOrAddEntry(string topicFilter)
   {
      return _subscriptions.GetOrAdd(topicFilter, static filter => new TopicSubscriptionEntry(filter));
   }

   private async ValueTask TrySubscribeBrokerAsync(string topicFilter, QualityOfServiceType qos, CancellationToken ct)
   {
      try
      {
         var options = SubscribeOptions.Create()
            .WithTopicFilter(topicFilter, qos)
            .Build();

         var result = await _client.SubscribeAsync(options, ct).ConfigureAwait(false);
         if (result.Failed)
         {
            TraceLogger.LogClientError("MqttSubscriptionManager: Broker subscribe failed for topic '{0}': {1}", topicFilter, result.Error.Detail);
         }
      }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("MqttSubscriptionManager: Error subscribing to topic '{0}': {1}", topicFilter, ex.Message);
      }
   }

   private async ValueTask RemoveSinkAsync(string topicFilter, ISubscriptionSink sink)
   {
      if (!_subscriptions.TryGetValue(topicFilter, out var entry))
      {
         return;
      }

      var becameEmpty = entry.RemoveSink(sink);
      if (!becameEmpty)
      {
         return;
      }

      _subscriptions.TryRemove(topicFilter, out _);

      if (_client.IsConnected)
      {
         try
         {
            var unsub = UnsubscribeOptions.Create()
               .WithTopicFilter(topicFilter)
               .Build();

            await _client.UnsubscribeAsync(unsub).ConfigureAwait(false);
            TraceLogger.LogClientInfo("MqttSubscriptionManager: Unsubscribed from topic '{0}' as last listener was removed.", topicFilter);
         }
         catch (Exception ex)
         {
            TraceLogger.LogClientError("MqttSubscriptionManager: Error unsubscribing from topic '{0}': {1}", topicFilter, ex.Message);
         }
      }
   }

   private async ValueTask DispatchMessageAsync(MessageReceiveContext context, CancellationToken ct)
   {
      var topic = context.Message.Topic;
      if (string.IsNullOrEmpty(topic))
      {
         return;
      }

      foreach (var kvp in _subscriptions)
      {
         var entry = kvp.Value;
         if (MqttTopicMatcher.IsMatch(entry.TopicFilter, topic))
         {
            await entry.DispatchAsync(context, ct).ConfigureAwait(false);
         }
      }
   }

   private async ValueTask OnClientConnectedAsync(ClientConnectedContext context, CancellationToken ct)
   {
      var activeSubs = _subscriptions.Values
         .Where(e => e.HasActiveSinks)
         .ToList();

      if (activeSubs.Count == 0)
      {
         return;
      }

      TraceLogger.LogClientInfo("MqttSubscriptionManager: Client (re)connected. Resubscribing {0} active topic filters...", activeSubs.Count);

      try
      {
         var builder = SubscribeOptions.Create();
         foreach (var entry in activeSubs)
         {
            builder.WithTopicFilter(entry.TopicFilter, entry.MaxQos);
         }

         var result = await _client.SubscribeAsync(builder.Build(), ct).ConfigureAwait(false);
         if (result.Failed)
         {
            TraceLogger.LogClientError("MqttSubscriptionManager: Auto-resubscribe failed: {0}", result.Error.Detail);
         }
         else
         {
            TraceLogger.LogClientInfo("MqttSubscriptionManager: Auto-resubscribe succeeded for {0} topic filters.", activeSubs.Count);
         }
      }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("MqttSubscriptionManager: Exception during auto-resubscribe: {0}", ex.Message);
      }
   }

   public ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return ValueTask.CompletedTask;
      }

      _receiveHandlerToken.Dispose();
      _connectedHandlerToken.Dispose();

      foreach (var kvp in _subscriptions)
      {
         kvp.Value.Dispose();
      }

      _subscriptions.Clear();
      return ValueTask.CompletedTask;
   }

   private sealed class SubscriptionDisposable(Func<ValueTask> onDispose) : IAsyncDisposable
   {
      private Func<ValueTask>? _onDispose = onDispose;

      public async ValueTask DisposeAsync()
      {
         var action = Interlocked.Exchange(ref _onDispose, null);
         if (action is not null)
         {
            await action().ConfigureAwait(false);
         }
      }
   }

   private sealed class TopicSubscriptionEntry(string topicFilter) : IDisposable
   {
      public string TopicFilter { get; } = topicFilter;
      public QualityOfServiceType MaxQos { get; private set; } = QualityOfServiceType.AtMostOnce;

      public bool HasActiveSinks
      {
         get
         {
            lock (_lock)
            {
               return _sinks.Count > 0;
            }
         }
      }

      private readonly Lock _lock = new();
      private readonly List<ISubscriptionSink> _sinks = [];

      public bool AddSink(ISubscriptionSink sink, QualityOfServiceType qos)
      {
         lock (_lock)
         {
            var isFirst = _sinks.Count == 0;
            _sinks.Add(sink);
            if (qos > MaxQos)
            {
               MaxQos = qos;
            }
            return isFirst;
         }
      }

      public bool RemoveSink(ISubscriptionSink sink)
      {
         lock (_lock)
         {
            _sinks.Remove(sink);
            return _sinks.Count == 0;
         }
      }

      public async ValueTask DispatchAsync(MessageReceiveContext context, CancellationToken ct)
      {
         ISubscriptionSink[] snapshot;
         lock (_lock)
         {
            if (_sinks.Count == 0) return;
            snapshot = _sinks.ToArray();
         }

         foreach (var sink in snapshot)
         {
            try
            {
               await sink.DeliverAsync(context, ct).ConfigureAwait(false);
            }
            catch (Exception ex)
            {
               TraceLogger.LogClientError("MqttSubscriptionManager: Error delivering message to sink on topic '{0}': {1}", TopicFilter, ex.Message);
            }
         }
      }

      public void Dispose()
      {
         lock (_lock)
         {
            foreach (var sink in _sinks)
            {
               if (sink is IDisposable d)
               {
                  d.Dispose();
               }
            }
            _sinks.Clear();
         }
      }
   }

   private interface ISubscriptionSink
   {
      ValueTask DeliverAsync(MessageReceiveContext context, CancellationToken ct);
   }

   private sealed class RawChannelSubscriptionSink(
      ChannelWriter<MqttPublishMessage> writer,
      BoundedChannelFullMode fullMode) : ISubscriptionSink, IDisposable
   {
      public ValueTask DeliverAsync(MessageReceiveContext context, CancellationToken ct)
      {
         if (writer.TryWrite(context.Message))
         {
            return ValueTask.CompletedTask;
         }

         if (fullMode is BoundedChannelFullMode.Wait)
         {
            return WriteSlowAsync(context.Message, ct);
         }

         return ValueTask.CompletedTask;
      }

      private async ValueTask WriteSlowAsync(MqttPublishMessage msg, CancellationToken ct)
      {
         try
         {
            await writer.WriteAsync(msg, ct).ConfigureAwait(false);
         }
         catch (ChannelClosedException) { }
         catch (OperationCanceledException) { }
      }

      public void Dispose()
      {
         writer.TryComplete();
      }
   }

   private sealed class ChannelSubscriptionSink<T>(
      ChannelWriter<T> writer,
      IMqttPayloadDecoder<T> decoder,
      BoundedChannelFullMode fullMode) : ISubscriptionSink, IDisposable
   {
      public ValueTask DeliverAsync(MessageReceiveContext context, CancellationToken ct)
      {
         T decoded;
         try
         {
            decoded = decoder.Decode(context.Message.Payload);
         }
         catch (Exception ex)
         {
            TraceLogger.LogClientError("MqttSubscriptionManager: Error decoding payload for topic '{0}': {1}", context.Message.Topic, ex.Message);
            return ValueTask.CompletedTask;
         }

         if (writer.TryWrite(decoded))
         {
            return ValueTask.CompletedTask;
         }

         if (fullMode is BoundedChannelFullMode.Wait)
         {
            return WriteSlowAsync(decoded, ct);
         }

         return ValueTask.CompletedTask;
      }

      private async ValueTask WriteSlowAsync(T item, CancellationToken ct)
      {
         try
         {
            await writer.WriteAsync(item, ct).ConfigureAwait(false);
         }
         catch (ChannelClosedException) { }
         catch (OperationCanceledException) { }
      }

      public void Dispose()
      {
         writer.TryComplete();
      }
   }

   private sealed class CallbackSubscriptionSink<T>(
      Func<T, MessageReceiveContext, CancellationToken, ValueTask> handler,
      IMqttPayloadDecoder<T> decoder) : ISubscriptionSink
   {
      public ValueTask DeliverAsync(MessageReceiveContext context, CancellationToken ct)
      {
         T decoded;
         try
         {
            decoded = decoder.Decode(context.Message.Payload);
         }
         catch (Exception ex)
         {
            TraceLogger.LogClientError("MqttSubscriptionManager: Error decoding payload for topic '{0}': {1}", context.Message.Topic, ex.Message);
            return ValueTask.CompletedTask;
         }

         return handler(decoded, context, ct);
      }
   }

   private sealed class RawCallbackSubscriptionSink(
      Func<MessageReceiveContext, CancellationToken, ValueTask> handler) : ISubscriptionSink
   {
      public ValueTask DeliverAsync(MessageReceiveContext context, CancellationToken ct)
      {
         return handler(context, ct);
      }
   }
}
