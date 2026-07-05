using System.Runtime.CompilerServices;

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

            var pingResult = await PingAsync(combined.Token);
            if (pingResult.Failed)
            {
               throw new Exception($"Keep-alive ping failed: {pingResult.Error.Detail}");
            }
         }
      }
      catch (OperationCanceledException)
      {
         // expected
      }
      catch (Exception)
      {
         if (_gracefulDisconnect)
         {
            return;
         }

         await DisconnectInternalAsync();
      }
   }
}
