using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Server.Internal;

public sealed class MqttKeepAliveService(
   MqttServer server)
{
   private readonly MqttServer _server = server;

   private CancellationTokenSource? _cancellationTokenSource;
   private Task? _runningTask;

   public void Start()
   {
      if (_runningTask is not null && !_runningTask.IsCompleted)
      {
         return;
      }

      _cancellationTokenSource = new CancellationTokenSource();
      _runningTask = Task.Run(() => RunWorkTask(_cancellationTokenSource.Token));
   }

   public async Task StopAsync()
   {
      if (_cancellationTokenSource is not null)
      {
         await _cancellationTokenSource.CancelAsync();
         _cancellationTokenSource = null;
      }

      if (_runningTask is not null)
      {
         try
         {
            await _runningTask.ConfigureAwait(false);
         }
         catch (OperationCanceledException)
         {
            // Expected during graceful shutdown
         }
      }
   }

   private async Task RunWorkTask(CancellationToken ct)
   {
      try
      {
         using var timer = new PeriodicTimer(_server.Options.KeepAlive.Interval);

         while (await timer.WaitForNextTickAsync(ct))
         {
            try
            {
               await _server.ClientSessions.CleanupExpiredSessionsAsync();
            }
            catch (Exception err)
            {
               TraceLogger.LogServerInfo("Error in MqttKeepAliveService session expiration cleanup: {0}", err.ToString());
            }

            using var clients = await _server.ClientSessions.GetClients();
            var now = DateTimeOffset.UtcNow;

            foreach (var client in clients.WrittenSpan)
            {
               try
               {
                  RunClient(client, now, ct);
               }
               catch (Exception err)
               {
                  TraceLogger.LogServerInfo("Error at MqttKeepAliveService task at client: {0}", err.ToString());
               }
            }
         }
      }
      catch (OperationCanceledException)
      {
         // expected
      }
      catch (Exception err)
      {
         // Unexpected exception
         TraceLogger.LogServerInfo("Error at MqttKeepAliveService task: {0}", err.ToString());
      }
      finally
      {
         TraceLogger.LogServerInfo("Stopped the MqttKeepAliveService BackgroundTask.");
      }
   }

   private static void RunClient(MqttServerClient client, DateTimeOffset now, CancellationToken ct)
   {
      if (!client.IsConnected) return;

      if (client.ConnectOptions is not { KeepAlivePeriod: > 0 }
          || client.Session is not { Stats: { } stats })
      {
         return;
      }

      if (stats.LastReceivedTimestamp is not { } receivedLast)
      {
         return;
      }

      var maxSecondsAllowed = client.ConnectOptions.KeepAlivePeriod * 1.5f;
      var difference = (now - receivedLast).TotalSeconds;

      if (difference < maxSecondsAllowed)
      {
         return;
      }

      _ = Task.Run(async () =>
      {
         try
         {
            await client.DisconnectAsync(_disconnectOptions);
         }
         catch (Exception err)
         {
            TraceLogger.LogServerInfo("Error disconnecting timed-out client: {0}", err.ToString());
         }
      }, ct);
   }

   private static readonly DisconnectOptions _disconnectOptions = new()
   {
      ReasonCode = DisconnectReasonCode.KeepAliveTimeout
   };
}
