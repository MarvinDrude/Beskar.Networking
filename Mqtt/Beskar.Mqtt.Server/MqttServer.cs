using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server.Enums;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Server;

/// <summary>
/// Runs a complete MQTT server.
/// </summary>
public sealed partial class MqttServer : IAsyncDisposable
{
   /// <summary>
   /// The current running state of the server.
   /// </summary>
   public MqttServerState State
   {
      get => (MqttServerState)_state;
      private set => _state = (int)value;
   }

   /// <summary>
   /// Whether the server is open to new connections.
   /// False = no new clients can connect.
   /// </summary>
   public bool OpenToNewConnections { get; set; } = true;

   private volatile bool _disposed;
   private volatile int _state = (int)MqttServerState.Stopped;

   private readonly INetworkListener[] _listeners;
   private CancellationTokenSource _cancellationTokenSource = new();

   internal MqttServer(INetworkListener[] listeners)
   {
      _listeners = listeners;
   }

   public async Task<VoidResult<StringError>> StartAsync()
   {
      if (_disposed)
         return new StringError("Already disposed server.");

      if (State is not MqttServerState.Stopped)
         return new StringError("Server is not in stopped state.");

      State = MqttServerState.Starting;

      await _cancellationTokenSource.CancelAsync();
      _cancellationTokenSource.Dispose();

      _cancellationTokenSource = new CancellationTokenSource();
      var ct = _cancellationTokenSource.Token;

      using var startedBuilder = new ArrayBuilder<INetworkListener>(_listeners.Length);

      foreach (var listener in _listeners)
      {
         var startResult = await listener.BindAsync(ct);
         _ = Task.Run(() => RunAcceptTask(listener, ct), ct);

         if (!startResult.Failed) continue;

         await CleanupCode(startedBuilder, ct);
         return new StringError($"Failed to start one of the listener: {startResult.Error.Message}");
      }

      State = MqttServerState.Running;
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

   public async Task<VoidResult<StringError>> StopAsync(DisconnectOptions? options = null)
   {
      if (_disposed)
         return new StringError("Already disposed server.");

      if (State is not MqttServerState.Running)
         return new StringError("Server is not running.");

      State = MqttServerState.Stopping;
      options ??= new DisconnectOptions()
      {
         ReasonCode = DisconnectReasonCode.ServerShuttingDown
      };

      await _cancellationTokenSource.CancelAsync();
      _cancellationTokenSource.Dispose();

      // notify clients

      foreach (var listener in _listeners)
      {
         await listener.UnbindAsync();
      }

      State = MqttServerState.Stopped;
      return true;
   }

   private async Task RunAcceptTask(INetworkListener listener, CancellationToken ct)
   {
      while (!ct.IsCancellationRequested)
      {
         try
         {
            var session = await listener.AcceptSessionAsync(ct);
            if (session.Failed) continue;

            _ = Task.Factory.StartNew(
               () => RunClientTask(listener, session.Success, ct), TaskCreationOptions.PreferFairness);
         }
         catch (Exception)
         {
            // ignored
         }
      }
   }

   private async Task RunClientTask(INetworkListener listener, INetworkSession session, CancellationToken ct)
   {
      if (ct.IsCancellationRequested) return;
      if (State is MqttServerState.Stopping or MqttServerState.Stopped) return;
      if (OpenToNewConnections) return;


   }

   public async ValueTask DisposeAsync()
   {
      if (_disposed) return;
      _disposed = true;

      await StopAsync();

      foreach (var listener in _listeners)
      {
         await listener.DisposeAsync();
      }
   }
}
