using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Memory.Writers;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Protocol;
using Beskar.Networking.Resilient.Common.Enums;
using Beskar.Networking.Resilient.Common.Interfaces;
using Beskar.Networking.Resilient.Server.Services;

namespace Beskar.Networking.Resilient.Server;

public sealed class ResilientServer<TFrame>
   : IResilientServer<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   public ResilientServerState State
   {
      get => (ResilientServerState)_state;
      private set => _state = (int)value;
   }

   public bool IsRunning
      => State is ResilientServerState.Running;

   public IReadOnlyList<INetworkListener> Listeners
      => _listeners;

   public ResilientServerOptions Options { get; }

   private int _disposedState; // 0 = not disposed, 1 = disposed
   private volatile int _state = (int)ResilientServerState.Stopped;

   private readonly INetworkListener[] _listeners;
   private CancellationTokenSource _cancellationTokenSource = new();

   private readonly ResilientKeepAliveService<TFrame> _keepAliveService;

   public ResilientServer(INetworkListener[] listeners, ResilientServerOptions options)
   {
      _listeners = listeners;
      Options = options;

      _keepAliveService = new ResilientKeepAliveService<TFrame>(this);
   }

   public async Task<VoidResult<StringError>> StartAsync()
   {
      if (Volatile.Read(ref _disposedState) == 1)
         return new StringError("Already disposed server.");

      if (State is not ResilientServerState.Stopped)
         return new StringError("Server is not running.");

      State = ResilientServerState.Starting;

      try
      {
         await _cancellationTokenSource.CancelAsync();
         _cancellationTokenSource.Dispose();
      }
      catch (ObjectDisposedException)
      {
         // already disposed
      }

      _cancellationTokenSource = new CancellationTokenSource();
      var ct = _cancellationTokenSource.Token;

      using var startedBuilder = new ArrayBuilder<INetworkListener>(_listeners.Length);

      foreach (var listener in _listeners)
      {
         var startResult = await listener.BindAsync(ct);
         _ = Task.Run(() => RunAcceptTask(listener, ct), ct);

         if (!startResult.Failed)
         {
            startedBuilder.Add(listener);
            continue;
         }

         await CleanupCode(startedBuilder, ct);
         return new StringError($"Failed to start one of the listener: {startResult.Error.Message}");
      }

      await _keepAliveService.StartAsync();
      State = ResilientServerState.Running;

      return true;

      static async Task CleanupCode(ArrayBuilder<INetworkListener> builder, CancellationToken ct)
      {
         var cleanups = builder.WrittenSpan.ToArray();
         foreach (var cleanup in cleanups)
         {
            await cleanup.UnbindAsync(ct);
         }
      }
   }

   public async Task<VoidResult<StringError>> StopAsync()
   {
      if (Volatile.Read(ref _disposedState) == 1)
         return new StringError("Already disposed server.");

      if (State is not ResilientServerState.Running)
         return new StringError("Server is not running.");

      State = ResilientServerState.Stopping;

      try
      {
         await _keepAliveService.StopAsync();
      }
      catch (Exception)
      {
         // ignored
      }

      try
      {
         await _cancellationTokenSource.CancelAsync();
         _cancellationTokenSource.Dispose();
      }
      catch (ObjectDisposedException)
      {
         // already disposed
      }

      foreach (var listener in _listeners)
      {
         await listener.UnbindAsync();
      }

      State = ResilientServerState.Stopped;

      return true;
   }

   private async Task RunAcceptTask(INetworkListener listener, CancellationToken ct)
   {

   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposedState, 1) == 1) return;

      await StopAsync();

      foreach (var listener in _listeners)
      {
         await listener.DisposeAsync();
      }

      await _keepAliveService.DisposeAsync();
   }
}
