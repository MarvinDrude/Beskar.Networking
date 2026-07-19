using System.Runtime.CompilerServices;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient
{
   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   private void ResetKeepAliveTimestamp()
   {
      _lastKeepAliveTimestamp = DateTimeOffset.UtcNow;
   }

   private async Task RunKeepAliveTask(TimeSpan keepAliveInterval, CancellationToken ct)
   {
      try
      {
         TraceLogger.LogClientInfo("MqttClient: Keep-alive task started (Interval: {0}ms).", keepAliveInterval.TotalMilliseconds);
         _lastKeepAliveTimestamp = DateTimeOffset.UtcNow;

         while (!ct.IsCancellationRequested)
         {
            var diff = DateTimeOffset.UtcNow - _lastKeepAliveTimestamp;
            if (diff < keepAliveInterval)
            {
               await Task.Delay(200, ct);
               continue;
            }

            using var combined = CancellationTokenSource.CreateLinkedTokenSource(ct);
            combined.CancelAfter(keepAliveInterval / 2);

            TraceLogger.LogClientInfo("MqttClient: Sending keep-alive PINGREQ...");
            var pingResult = await PingAsync(combined.Token);

            if (pingResult.Failed)
            {
               ct.ThrowIfCancellationRequested();
               throw new Exception($"Keep-alive ping failed: {pingResult.Error.Detail}");
            }

            TraceLogger.LogClientInfo("MqttClient: Keep-alive PINGRESP received successfully.");
         }
      }
      catch (OperationCanceledException)
      {
         TraceLogger.LogClientInfo("MqttClient: Keep-alive task stopped (cancelled).");
      }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("MqttClient: Keep-alive task error: {0}", ex.Message);
         if (_gracefulDisconnect)
         {
            return;
         }

         await DisconnectInternalAsync(awaitReceiveTask: true, awaitKeepAliveTask: false);
      }
   }
}
