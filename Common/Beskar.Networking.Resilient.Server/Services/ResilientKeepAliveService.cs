using Beskar.Networking.Protocol;
using Beskar.Networking.Resilient.Common.Enums;

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
      if (_cts != null)
      {
         await _cts.CancelAsync();
         _cts.Dispose();
         _cts = null;
      }

      if (_timerTask != null)
      {
         try
         {
            await _timerTask;
         }
         catch (OperationCanceledException)
         {
            // expected
         }
         _timerTask = null;
      }
   }

   private async Task RunKeepAliveLoopAsync(CancellationToken ct)
   {
      var options = _server.Options.KeepAlive;
      if (options.Mode is ResilientServerKeepAliveMode.None)
         return;

      using var timer = new PeriodicTimer(options.CheckInterval);

      while (!ct.IsCancellationRequested && await timer.WaitForNextTickAsync(ct))
      {
         var now = DateTimeOffset.UtcNow;
         var clients = _server.Clients.GetAll();

         foreach (var client in clients)
         {
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

   public async ValueTask DisposeAsync()
   {
      await StopAsync();
   }
}
