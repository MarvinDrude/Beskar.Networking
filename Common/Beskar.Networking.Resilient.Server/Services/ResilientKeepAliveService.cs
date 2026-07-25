using Beskar.Networking.Protocol;
using Beskar.Networking.Resilient.Common.Enums;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Resilient.Server.Services;

/// <summary>
/// Background service that monitors and handles idle client keep-alive checks.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientKeepAliveService<TFrame>(ResilientServer<TFrame> server)
   : IAsyncDisposable
   where TFrame : struct, IFramingProtocol<TFrame>
{
   private readonly ResilientServer<TFrame> _server = server;
   private CancellationTokenSource? _cts;
   private Task? _timerTask;

   public Task StartAsync()
   {
      if (_timerTask != null) return Task.CompletedTask;

      _cts = new CancellationTokenSource();
      var token = _cts.Token;
      // ReSharper disable once MethodSupportsCancellation
      _timerTask = Task.Run(() => RunKeepAliveLoopAsync(token));
      return Task.CompletedTask;
   }

   public async Task StopAsync()
   {
      var cts = _cts;
      _cts = null;

      if (cts != null)
      {
         try
         {
            await cts.CancelAsync();
         }
         catch
         {
            // ignored
         }
      }

      if (_timerTask != null)
      {
         try
         {
            await _timerTask;
         }
         catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException)
         {
            // expected
         }
         _timerTask = null;
      }

      cts?.Dispose();
   }

   private async Task RunKeepAliveLoopAsync(CancellationToken ct)
   {
      var options = _server.Options.KeepAlive;
      if (options.Mode is ResilientServerKeepAliveMode.None)
         return;

      using var timer = new PeriodicTimer(options.CheckInterval);

      try
      {
         while (!ct.IsCancellationRequested && await timer.WaitForNextTickAsync(ct))
         {
            var now = DateTimeOffset.UtcNow;
            var clients = _server.Clients.GetAll();

            foreach (var client in clients)
            {
               if (!client.IsHandshakeCompleted)
               {
                  continue;
               }

               if (options.Mode is ResilientServerKeepAliveMode.ClientConfigured &&
                   client.ConnectPayload is { KeepAliveSeconds: 0 })
               {
                  // Client explicitly disabled keep-alive
                  continue;
               }

               var baseTimeout = options.Mode is ResilientServerKeepAliveMode.ClientConfigured
                  && client.ConnectPayload is { KeepAliveSeconds: > 0 }
                  ? TimeSpan.FromSeconds(client.ConnectPayload.KeepAliveSeconds)
                  : options.DefaultKeepAliveTime;

               var timeout = baseTimeout * 1.5;

               if (now - client.LastActivityAt > timeout)
               {
                  TraceLogger.LogServerWarning("ResilientServer KeepAlive: Client {0} idle for {1:F1}s (exceeds timeout {2:F1}s). Disconnecting client.", client.Id, (now - client.LastActivityAt).TotalSeconds, timeout.TotalSeconds);
                  if (_server.Clients.TryRemove(client.Id, out _))
                  {
                     _ = Task.Run(async () =>
                     {
                        try
                        {
                           await client.DisconnectAsync();
                        }
                        catch
                        {
                           // ignored
                        }
                     }, CancellationToken.None);
                  }
               }
            }
         }
      }
      catch (Exception ex) when (ex is OperationCanceledException or ObjectDisposedException)
      {
         // expected on cancellation or dispose
      }
   }

   public async ValueTask DisposeAsync()
   {
      await StopAsync();
   }
}
