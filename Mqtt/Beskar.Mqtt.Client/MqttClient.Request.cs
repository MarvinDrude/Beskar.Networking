using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Models;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient
{
   private readonly ConcurrentDictionary<string, TaskCompletionSource<MqttPublishMessage>> _pendingRequests = new();
   private readonly ConcurrentDictionary<string, byte> _subscribedResponseTopics = new();

   internal bool TryDispatchResponse(MqttPublishMessage message)
   {
      if (message.CorrelationData is { IsEmpty: false })
      {
         var correlationKey = Convert.ToBase64String(message.CorrelationData.Value.Span);
         if (_pendingRequests.TryRemove(correlationKey, out var tcs))
         {
            return tcs.TrySetResult(message);
         }
      }

      return false;
   }

   internal void CancelPendingRequestsOnDisconnect()
   {
      _subscribedResponseTopics.Clear();

      foreach (var kvp in _pendingRequests)
      {
         if (_pendingRequests.TryRemove(kvp.Key, out var tcs))
         {
            tcs.TrySetException(new OperationCanceledException("Client disconnected while awaiting request response."));
         }
      }
   }

   public async Task<Result<MqttResponseContext, StringError>> RequestAsync(
      PublishOptions options, TimeSpan timeout = default, CancellationToken ct = default)
   {
      var validRes = ValidateClient();
      if (validRes.Failed) return validRes.Error;

      if (_protocolVersion is not MqttProtocolVersion.V50)
      {
         return new StringError("RequestAsync requires MQTT 5.0 protocol version for ResponseTopic and CorrelationData support.");
      }

      if (timeout == TimeSpan.Zero || timeout <= TimeSpan.Zero)
      {
         timeout = TimeSpan.FromSeconds(10);
      }

      string responseTopic;
      string correlationKey;

      PublishOptions effectiveOptions;

      var needsNewTopic = options.ResponseTopicUtf8Bytes.IsEmpty;
      var needsNewCorr = options.CorrelationData.IsEmpty;

      if (needsNewTopic || needsNewCorr)
      {
         var builder = PublishOptions.Create()
            .WithDup(options.Dup)
            .WithTopic(options.TopicUtf8Bytes)
            .WithPayload(options.Payload)
            .WithQualityOfService(options.QualityOfService)
            .WithRetain(options.Retain)
            .WithPayloadFormat(options.PayloadFormat);

         if (options.MessageExpiryInterval.HasValue)
         {
            builder.WithMessageExpiryInterval(options.MessageExpiryInterval.Value);
         }

         if (options.TopicAlias.HasValue)
         {
            builder.WithTopicAlias(options.TopicAlias.Value);
         }

         if (!options.ContentTypeUtf8Bytes.IsEmpty)
         {
            builder.WithContentType(options.ContentTypeUtf8Bytes);
         }

         if (options.UserProperties.Count > 0)
         {
            foreach (var prop in options.UserProperties)
            {
               builder.WithUserProperty(prop.KeyUtf8Bytes, prop.ValueBytes);
            }
         }

         if (options.SubscriptionIdentifiers.Count > 0)
         {
            foreach (var subId in options.SubscriptionIdentifiers)
            {
               builder.WithSubscriptionIdentifier(subId);
            }
         }

         if (needsNewTopic)
         {
            var clientIdStr = Encoding.UTF8.GetString(_connectOptions.ClientIdUtf8Bytes.Span);
            if (string.IsNullOrEmpty(clientIdStr))
            {
               clientIdStr = Guid.NewGuid().ToString("N");
            }
            responseTopic = $"clients/{clientIdStr}/response";
            builder.WithResponseTopic(responseTopic);
         }
         else
         {
            responseTopic = Encoding.UTF8.GetString(options.ResponseTopicUtf8Bytes.Span);
            builder.WithResponseTopic(options.ResponseTopicUtf8Bytes);
         }

         if (needsNewCorr)
         {
            correlationKey = Guid.NewGuid().ToString("N");
            var corrBytes = Encoding.UTF8.GetBytes(correlationKey);

            builder.WithCorrelationData(corrBytes);
            correlationKey = Convert.ToBase64String(corrBytes);
         }
         else
         {
            correlationKey = Convert.ToBase64String(options.CorrelationData.Span);
            builder.WithCorrelationData(options.CorrelationData);
         }

         effectiveOptions = builder.Build();
      }
      else
      {
         responseTopic = Encoding.UTF8.GetString(options.ResponseTopicUtf8Bytes.Span);
         correlationKey = Convert.ToBase64String(options.CorrelationData.Span);
         effectiveOptions = options;
      }

      var tcs = new TaskCompletionSource<MqttPublishMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
      if (!_pendingRequests.TryAdd(correlationKey, tcs))
      {
         return new StringError($"A pending request with correlation ID '{correlationKey}' is already in progress.");
      }

      var startTimestamp = Stopwatch.GetTimestamp();

      try
      {
         if (!_subscribedResponseTopics.ContainsKey(responseTopic))
         {
            var subOptions = SubscribeOptions.Create()
               .WithTopicFilter(responseTopic, QualityOfServiceType.AtLeastOnce)
               .Build();

            var subResult = await SubscribeAsync(subOptions, ct);
            if (subResult.Failed)
            {
               return subResult.Error;
            }

            _subscribedResponseTopics[responseTopic] = 1;
         }

         var pubResult = await PublishAsync(effectiveOptions, ct);
         if (pubResult.Failed)
         {
            return pubResult.Error;
         }

         using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
         combinedCts.CancelAfter(timeout);

         MqttPublishMessage responseMessage;
         try
         {
            responseMessage = await tcs.Task.WaitAsync(combinedCts.Token);
         }
         catch (OperationCanceledException ex)
         {
            if (ct.IsCancellationRequested)
            {
               return new StringError("Request was cancelled.");
            }

            if (!IsConnected)
            {
               return new StringError($"Request cancelled: {ex.Message}");
            }

            return new StringError(
               $"Request timed out after {timeout.TotalMilliseconds}ms waiting for response on topic '{responseTopic}'.");
         }

         var elapsedTicks = Stopwatch.GetTimestamp() - startTimestamp;
         var elapsed = TimeSpan.FromSeconds((double)elapsedTicks / Stopwatch.Frequency);

         return new MqttResponseContext
         {
            Message = responseMessage,
            CorrelationId = correlationKey,
            Elapsed = elapsed
         };
      }
      finally
      {
         _pendingRequests.TryRemove(correlationKey, out _);
      }
   }

   public Task<Result<MqttResponseContext, StringError>> RequestAsync(
      string topic, ReadOnlyMemory<byte> payload, TimeSpan timeout = default, CancellationToken ct = default)
   {
      var options = PublishOptions.Create()
         .WithTopic(topic)
         .WithPayload(payload)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .Build();

      return RequestAsync(options, timeout, ct);
   }
}
