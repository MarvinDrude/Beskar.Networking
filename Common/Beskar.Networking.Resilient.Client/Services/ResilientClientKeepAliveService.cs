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
      var options = _client.Options.KeepAlive;
      if (!options.Enabled || options.KeepAliveInterval <= TimeSpan.Zero)
         return;

      var minIntervalMs = options.Timeout > TimeSpan.Zero
         ? Math.Min(options.KeepAliveInterval.TotalMilliseconds, options.Timeout.TotalMilliseconds)
         : options.KeepAliveInterval.TotalMilliseconds;
      var pollDelayMs = (int)Math.Clamp(minIntervalMs / 4.0, 10.0, 200.0);

      try
      {
         while (!ct.IsCancellationRequested)
         {
            if (!_client.IsConnected)
            {
               await Task.Delay(pollDelayMs, ct);
               continue;
            }

            var now = DateTimeOffset.UtcNow;

            if (Volatile.Read(ref _client.AwaitingPong) == 1)
            {
               var pingSentTicks = Volatile.Read(ref _client.LastPingSentTicks);
               if (options.Timeout > TimeSpan.Zero &&
                   now.UtcTicks - pingSentTicks >= options.Timeout.Ticks)
               {
                  Volatile.Write(ref _client.AwaitingPong, 0);
                  _client.HandleKeepAliveTimeout();
                  return;
               }

               await Task.Delay(pollDelayMs, ct);
               continue;
            }

            var diff = now - _client.LastActivityAt;

            if (diff < options.KeepAliveInterval)
            {
               await Task.Delay(pollDelayMs, ct);
               continue;
            }

            try
            {
               Volatile.Write(ref _client.LastPingSentTicks, DateTimeOffset.UtcNow.Ticks);
               Volatile.Write(ref _client.AwaitingPong, 1);

               var pingFrame = TFrame.CreateFrame(ResilientFrameKind.Ping);
               await _client.SendAsync(pingFrame, ct);
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
               // protection against keep-alive send exceptions
            }

            await Task.Delay(pollDelayMs, ct);
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
