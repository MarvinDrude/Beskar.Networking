using System.Collections.Concurrent;
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

   internal bool TryDispatchResponse(MqttPublishMessage message)
   {
      if (message.CorrelationData.HasValue)
      {
         var correlationId = Encoding.UTF8.GetString(message.CorrelationData.Value.Span);
         if (_pendingRequests.TryRemove(correlationId, out var tcs))
         {
            return tcs.TrySetResult(message);
         }
      }

      return false;
   }

   public async Task<Result<MqttResponseContext, StringError>> RequestAsync(
      PublishOptions options, TimeSpan timeout = default, CancellationToken ct = default)
   {
      var validRes = ValidateClient();
      if (validRes.Failed) return validRes.Error;

      if (timeout == TimeSpan.Zero || timeout <= TimeSpan.Zero)
      {
         timeout = TimeSpan.FromSeconds(10);
      }

      string responseTopic;
      if (options.ResponseTopicUtf8Bytes.IsEmpty)
      {
         var clientIdStr = Encoding.UTF8.GetString(_connectOptions.ClientIdUtf8Bytes.Span);
         if (string.IsNullOrEmpty(clientIdStr))
         {
            clientIdStr = Guid.NewGuid().ToString("N");
         }

         responseTopic = $"clients/{clientIdStr}/response";
         options.ResponseTopicUtf8Bytes = Encoding.UTF8.GetBytes(responseTopic);
      }
      else
      {
         responseTopic = Encoding.UTF8.GetString(options.ResponseTopicUtf8Bytes.Span);
      }

      string correlationId;
      if (options.CorrelationData.IsEmpty)
      {
         correlationId = Guid.NewGuid().ToString("N");
         options.CorrelationData = Encoding.UTF8.GetBytes(correlationId);
      }
      else
      {
         correlationId = Encoding.UTF8.GetString(options.CorrelationData.Span);
      }

      // Auto-subscribe client to response topic
      var subOptions = SubscribeOptions.Create()
         .WithTopicFilter(responseTopic, QualityOfServiceType.AtLeastOnce)
         .Build();

      var subResult = await SubscribeAsync(subOptions, ct);
      if (subResult.Failed)
      {
         return subResult.Error;
      }

      var tcs = new TaskCompletionSource<MqttPublishMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
      _pendingRequests[correlationId] = tcs;

      var startTime = DateTime.UtcNow;

      try
      {
         var pubResult = await PublishAsync(options, ct);
         if (pubResult.Failed)
         {
            _pendingRequests.TryRemove(correlationId, out _);
            return pubResult.Error;
         }

         using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
         combinedCts.CancelAfter(timeout);

         MqttPublishMessage responseMessage;
         try
         {
            responseMessage = await tcs.Task.WaitAsync(combinedCts.Token);
         }
         catch (OperationCanceledException)
         {
            _pendingRequests.TryRemove(correlationId, out _);
            if (ct.IsCancellationRequested)
            {
               return new StringError("Request was cancelled.");
            }

            return new StringError(
               $"Request timed out after {timeout.TotalMilliseconds}ms waiting for response on topic '{responseTopic}'.");
         }

         var elapsed = DateTime.UtcNow - startTime;
         return new MqttResponseContext
         {
            Message = responseMessage,
            CorrelationId = correlationId,
            Elapsed = elapsed
         };
      }
      finally
      {
         _pendingRequests.TryRemove(correlationId, out _);
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
