using Beskar.Networking.Protocol;

namespace Beskar.Networking.Resilient.Client.Services;

/// <summary>
/// Background service that manages client keep-alive pings to the server.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientClientKeepAliveService<TFrame>(ResilientClient<TFrame> client)
   : IAsyncDisposable
   where TFrame : struct, IFramingProtocol<TFrame>
{
   private readonly ResilientClient<TFrame> _client = client;
   private CancellationTokenSource? _cts;
   private Task? _timerTask;

   public Task StartAsync()
   {
      if (_timerTask != null) return Task.CompletedTask;

      _cts = new CancellationTokenSource();
      _timerTask = Task.Run(() => RunKeepAliveLoopAsync(_cts.Token));
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
      var options = _client.Options.KeepAlive;
      if (!options.Enabled || options.KeepAliveInterval <= TimeSpan.Zero)
         return;

      using var timer = new PeriodicTimer(options.KeepAliveInterval);

      while (!ct.IsCancellationRequested && await timer.WaitForNextTickAsync(ct))
      {
         if (!_client.IsConnected) continue;

         var now = DateTimeOffset.UtcNow;
         var idleTime = now - _client.LastActivityAt;

         if (idleTime >= options.KeepAliveInterval)
         {
            var pingFrame = TFrame.CreateFrame(ResilientFrameKind.Ping);
            await _client.SendAsync(pingFrame, ct);
         }
      }
   }

   public async ValueTask DisposeAsync()
   {
      await StopAsync();
   }
}
