using System.Net;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Options;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Abstractions.Managed;

/// <summary>
/// A managed, high-level client wrapper that supports automatic reconnection.
/// </summary>
public sealed class NetworkClient : INetworkClient, IAsyncDisposable
{
   private const int StateDisconnected = (int)ConnectionState.Disconnected;
   private const int StateConnecting = (int)ConnectionState.Connecting;
   private const int StateConnected = (int)ConnectionState.Connected;
   private const int StateReconnecting = (int)ConnectionState.Reconnecting;
   private const int StateFailed = (int)ConnectionState.Failed;

   private readonly INetworkClient _innerClient;
   private readonly AutoReconnectOptions _options;

   private INetworkSession? _currentSession;
   private int _stateInt = StateDisconnected;
   private EndPoint? _lastEndPoint;
   private CancellationTokenSource? _clientLifetimeCts;
   private CancellationTokenRegistration _sessionClosedRegistration;
   private Task? _reconnectTask;

   /// <summary>
   /// Occurs when the client successfully connects or reconnects to the endpoint.
   /// </summary>
   public event Action<INetworkSession>? Connected;

   /// <summary>
   /// Occurs when the client is disconnected.
   /// </summary>
   public event Action? Disconnected;

   /// <summary>
   /// Occurs when the client is starting a reconnection attempt.
   /// Passes the attempt index (1-based) and the delay duration.
   /// </summary>
   public event Action<int, TimeSpan>? Reconnecting;

   /// <summary>
   /// Occurs when the client fails to connect after the configured maximum retry attempts.
   /// </summary>
   public event Action<NetworkCodeError>? ConnectionFailed;

   /// <summary>
   /// Occurs when the connection state of the client changes.
   /// Passes (oldState, newState).
   /// </summary>
   public event Action<ConnectionState, ConnectionState>? StateChanged;

   /// <summary>
   /// The current connection state of the client.
   /// </summary>
   public ConnectionState State => (ConnectionState)Volatile.Read(ref _stateInt);

   /// <summary>
   /// Gets the active network session, or null if not connected.
   /// </summary>
   public INetworkSession? Session => Volatile.Read(ref _currentSession);

   /// <summary>
   /// Initializes a new instance of the <see cref="NetworkClient"/> class.
   /// </summary>
   public NetworkClient(INetworkClient innerClient, AutoReconnectOptions? options = null)
   {
      _innerClient = innerClient ?? throw new ArgumentNullException(nameof(innerClient));
      _options = options ?? new AutoReconnectOptions();
   }

   /// <summary>
   /// Connects to the specified remote endpoint.
   /// </summary>
   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint, CancellationToken ct = default)
   {
      var current = Session;
      if (State == ConnectionState.Connected && current is not null)
      {
         return new Result<INetworkSession, NetworkCodeError>(current);
      }

      if (!TryTransitionState(StateDisconnected, StateConnecting))
      {
         return new NetworkCodeError(-1, "Client is already connecting, reconnecting, or connected.");
      }

      _lastEndPoint = endPoint;

      var newCts = new CancellationTokenSource();
      var oldCts = Interlocked.Exchange(ref _clientLifetimeCts, newCts);

      if (oldCts is not null)
      {
         await oldCts.CancelAsync();
         oldCts.Dispose();
      }

      using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, newCts.Token);

      TraceLogger.LogClientInfo("ManagedClient ConnectAsync: Initiating connection to {0}", endPoint);
      var result = await _innerClient.ConnectAsync(endPoint, linkedCts.Token);

      if (result.IsSuccess)
      {
         var session = result.Success!;
         if (newCts.Token.IsCancellationRequested)
         {
            TryTransitionState(StateConnecting, StateDisconnected);
            await DisposeSession(session);

            return new NetworkCodeError(-1, "Connection attempt was canceled.");
         }

         var oldSession = Interlocked.Exchange(ref _currentSession, session);
         if (oldSession is not null)
         {
            await DisposeSession(oldSession);
         }

         SetState(StateConnected);

         await _sessionClosedRegistration.DisposeAsync();
         _sessionClosedRegistration = session.SessionClosedToken.Register(() => OnSessionClosed(session));

         TraceLogger.LogClientInfo("ManagedClient ConnectAsync: Connected successfully. Session ID: {0}", session.Id);
         Connected?.Invoke(session);

         return result;
      }

      TryTransitionState(StateConnecting, StateDisconnected);

      TraceLogger.LogClientError("ManagedClient ConnectAsync: Connection attempt failed: {0}", result.Error.Message);

      if (!_options.IsEnabled) return result.Error;
      if (TryTransitionState(StateDisconnected, StateReconnecting))
      {
         StartReconnectLoop(endPoint);
      }

      return result.Error;
   }

   /// <summary>
   /// Disconnects the client, cancels any active reconnection attempt, and disposes the current session.
   /// </summary>
   public async ValueTask DisconnectAsync()
   {
      var currentState = Volatile.Read(ref _stateInt);
      while (currentState != StateDisconnected)
      {
         var oldState = Interlocked.CompareExchange(ref _stateInt, StateDisconnected, currentState);
         if (oldState == currentState)
         {
            StateChanged?.Invoke((ConnectionState)currentState, ConnectionState.Disconnected);
            break;
         }

         currentState = oldState;
      }

      var cts = Interlocked.Exchange(ref _clientLifetimeCts, null);
      if (cts is not null)
      {
         await cts.CancelAsync();
         cts.Dispose();
      }

      var session = Interlocked.Exchange(ref _currentSession, null);
      if (session is not null)
      {
         await DisposeSession(session);
      }

      await _sessionClosedRegistration.DisposeAsync();

      var task = Interlocked.Exchange(ref _reconnectTask, null);
      if (task is not null)
      {
         try
         {
            await task;
         }
         catch
         {
            // Ignore background reconnection task cancellation exceptions
         }
      }

      TraceLogger.LogClientInfo("ManagedClient DisconnectAsync: Client disconnected.");
      Disconnected?.Invoke();
   }

   /// <summary>
   /// Opens a new stream on the active network session.
   /// </summary>
   public async ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional, CancellationToken ct = default)
   {
      var session = Session;
      if (session is null || State != ConnectionState.Connected)
      {
         return new NetworkCodeError(-1, "Client is not connected.");
      }

      return await session.OpenStreamAsync(direction, ct);
   }

   /// <summary>
   /// Accepts an incoming stream on the active network session.
   /// </summary>
   public async ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
   {
      var session = Session;
      if (session is null || State != ConnectionState.Connected)
      {
         return new NetworkCodeError(-1, "Client is not connected.");
      }

      return await session.AcceptStreamAsync(ct);
   }

   private void OnSessionClosed(INetworkSession closedSession)
   {
      var trackedSession = Interlocked.CompareExchange(ref _currentSession, null, closedSession);
      if (trackedSession != closedSession)
      {
         return;
      }

      _sessionClosedRegistration.Dispose();

      if (TryTransitionState(StateConnected, StateReconnecting))
      {
         if (_options.IsEnabled && _lastEndPoint is not null)
         {
            StartReconnectLoop(_lastEndPoint);
         }
         else
         {
            SetState(StateDisconnected);
            Disconnected?.Invoke();
         }
      }
   }

   private void StartReconnectLoop(EndPoint endPoint)
   {
      var task = Volatile.Read(ref _reconnectTask);
      if (task is not null && !task.IsCompleted)
      {
         return;
      }

      var cts = Volatile.Read(ref _clientLifetimeCts);
      if (cts is null || cts.IsCancellationRequested)
      {
         var newCts = new CancellationTokenSource();
         var oldCts = Interlocked.Exchange(ref _clientLifetimeCts, newCts);

         if (oldCts is not null)
         {
            oldCts.Cancel();
            oldCts.Dispose();
         }

         cts = newCts;
      }

      var newTask = Task.Run(() => ReconnectLoopAsync(endPoint, cts.Token), cts.Token);
      var oldTask = Interlocked.Exchange(ref _reconnectTask, newTask);

      if (oldTask is not null && !oldTask.IsCompleted)
      {
         Interlocked.CompareExchange(ref _reconnectTask, oldTask, newTask);
      }
   }

   private async Task ReconnectLoopAsync(EndPoint endPoint, CancellationToken clientLifetimeToken)
   {
      var attempt = 0;

      while (!clientLifetimeToken.IsCancellationRequested)
      {
         attempt++;

         if (_options.MaxRetryAttempts != -1 && attempt > _options.MaxRetryAttempts)
         {
            SetState(StateFailed);

            TraceLogger.LogClientError("ManagedClient: Max reconnect attempts ({0}) reached. Stopping.", _options.MaxRetryAttempts);
            ConnectionFailed?.Invoke(new NetworkCodeError(-1, "Max reconnect attempts reached."));

            return;
         }

         var delay = _options.BackoffPolicy.GetNextDelay(attempt);
         Reconnecting?.Invoke(attempt, delay);

         TraceLogger.LogClientWarning("ManagedClient: Connection lost. Reconnect attempt {0} scheduled in {1}ms", attempt, delay.TotalMilliseconds);

         try
         {
            await Task.Delay(delay, clientLifetimeToken);
         }
         catch (OperationCanceledException)
         {
            return;
         }

         TraceLogger.LogClientInfo("ManagedClient: Reconnect attempt {0} connecting to {1}", attempt, endPoint);

         var result = await _innerClient.ConnectAsync(endPoint, clientLifetimeToken);
         if (result.IsSuccess)
         {
            var session = result.Success!;
            if (clientLifetimeToken.IsCancellationRequested)
            {
               await DisposeSession(session);
               return;
            }

            var oldSession = Interlocked.Exchange(ref _currentSession, session);
            if (oldSession is not null)
            {
               await DisposeSession(oldSession);
            }

            SetState(StateConnected);

            await _sessionClosedRegistration.DisposeAsync();
            _sessionClosedRegistration = session.SessionClosedToken.Register(() => OnSessionClosed(session));

            TraceLogger.LogClientInfo("ManagedClient: Reconnected successfully on attempt {0}. Session ID: {1}", attempt, session.Id);
            Connected?.Invoke(session);

            return;
         }

         TraceLogger.LogClientError("ManagedClient: Reconnect attempt {0} failed: {1}", attempt, result.Error.Message);
      }
   }

   private bool TryTransitionState(int expected, int newValue)
   {
      var old = Interlocked.CompareExchange(ref _stateInt, newValue, expected);

      if (old == expected)
      {
         StateChanged?.Invoke((ConnectionState)expected, (ConnectionState)newValue);
         return true;
      }

      return false;
   }

   private void SetState(int to)
   {
      var old = Interlocked.Exchange(ref _stateInt, to);

      if (old != to)
      {
         StateChanged?.Invoke((ConnectionState)old, (ConnectionState)to);
      }
   }

   private static async ValueTask DisposeSession(INetworkSession session)
   {
      try
      {
         if (session is IAsyncDisposable asyncDisposable)
         {
            await asyncDisposable.DisposeAsync();
         }
      }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("ManagedClient: Error disposing network session: {0}", ex.Message);
      }
   }

   /// <inheritdoc />
   public async ValueTask DisposeAsync()
   {
      await DisconnectAsync();
   }
}
