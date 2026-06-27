namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient
{
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

            _ = await PingAsync(combined.Token);
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
