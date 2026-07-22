using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Resilient.Common.Enums;
using Beskar.Networking.Resilient.Common.Interfaces;

namespace Beskar.Networking.Resilient.Server;

public sealed class ResilientServer : IResilientServer
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

   private volatile bool _disposed;
   private volatile int _state = (int)ResilientServerState.Stopped;

   private readonly INetworkListener[] _listeners;
   private CancellationTokenSource _cancellationTokenSource = new();

   public ResilientServer(INetworkListener[] listeners, ResilientServerOptions options)
   {
      _listeners = listeners;
      Options = options;
   }

   public async Task<VoidResult<StringError>> StartAsync()
   {

   }

   public async Task<VoidResult<StringError>> StopAsync()
   {

   }

   public async ValueTask DisposeAsync()
   {
      if (_disposed) return;
      _disposed = true;


   }
}
