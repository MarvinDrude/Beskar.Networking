using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Memory.Threading;
using Beskar.Mqtt.Client.Handlers;
using Beskar.Mqtt.Client.States;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Generators;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Common.Handlers.Client;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Models;
using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Client;

/// <summary>
/// Full MQTT client implementation.
/// </summary>
public sealed partial class MqttClient : IMqttClient, IMqttPacketSender
{
   /// <summary>
   /// Whether the client is connected to the server currently.
   /// </summary>
   public bool IsConnected => (MqttClientConnectionState)_state is MqttClientConnectionState.Connected;

   /// <summary>
   /// Event dispatcher container for client events.
   /// </summary>
   public ClientEvents Events { get; } = new();

   internal MqttProtocolVersion ProtocolVersion => _protocolVersion;
   internal ConnectOptions CurrentConnectOptions => _connectOptions;

   private readonly INetworkClient _networkClient;
   private INetworkSession? _networkSession;
   private INetworkStream? _controlStream;

   private readonly IPacketHandler _packetHandler;
   private readonly SignalBroker _signalBroker = new();
   private readonly PacketIdentifierGenerator _identifierGenerator = new();

   private volatile bool _disposed;
   private volatile bool _firstConnect = true;
   private volatile int _state = (int)MqttClientConnectionState.Disconnected;

   private volatile bool _gracefulDisconnect;
   private MqttClientDisconnectReason? _disconnectReason;
   private volatile Exception? _disconnectException;
   private UserPropertyCollection? _disconnectUserProperties;
   private string? _disconnectReasonString;

   private MqttProtocolVersion _protocolVersion = MqttProtocolVersion.Unknown;

   private CancellationTokenSource _clientTokenSource = new();

   private Task? _receiveTask;
   private Task? _keepAliveTask;
   private DateTimeOffset _lastKeepAliveTimestamp;

   private ConnectOptions _connectOptions = new() { EndPoint = new IPEndPoint(0, 0) };
   private readonly Dictionary<ushort, byte[]> _topicAliases = new(16);

   private SemaphoreSlim? _inFlightSemaphore;
   private int _incomingInFlightCount;

   public MqttClient(INetworkClient networkClient)
   {
      _networkClient = networkClient;
      _packetHandler = new ClientPacketHandler(this);

      _gracefulDisconnect = false;
   }

   public IDisposable AddMessageReceiveHandler(
      Func<MessageReceiveContext, CancellationToken, ValueTask> messageReceiveHandler)
   {
      return Events.OnMessageReceive.Add(messageReceiveHandler);
   }

   public IDisposable AddConnectingHandler(
      Func<ClientConnectingContext, CancellationToken, ValueTask> handler)
   {
      return Events.OnClientConnecting.Add(handler);
   }

   public IDisposable AddConnectedHandler(
      Func<ClientConnectedContext, CancellationToken, ValueTask> handler)
   {
      return Events.OnClientConnected.Add(handler);
   }

   public IDisposable AddDisconnectedHandler(
      Func<ClientDisconnectedContext, CancellationToken, ValueTask> handler)
   {
      return Events.OnClientDisconnected.Add(handler);
   }

   public async Task<Result<ClientConnectResult, StringError>> ConnectAsync(
      ConnectOptions options, CancellationToken ct = default)
   {
      var disposedResult = ValidateDisposed();
      if (disposedResult.Failed) return disposedResult.Error;

      if (CompareExchangeState(MqttClientConnectionState.Connecting, MqttClientConnectionState.Disconnected)
          is not MqttClientConnectionState.Disconnected)
      {
         return new StringError("ConnectAsync called while not being in disconnected state.");
      }

      TraceLogger.LogClientInfo("MqttClient.ConnectAsync: Connecting to {0} (ProtocolVersion: {1})", options.EndPoint, options.ProtocolVersion);

      try
      {
         _connectOptions = options;
         _protocolVersion = options.ProtocolVersion;
         _topicAliases.Clear();

         if (Events.OnClientConnecting.Count > 0)
         {
            var ctx = new ClientConnectingContext()
            {
               ConnectOptions = options
            };

            await Events.OnClientConnecting.ExecuteAsync(
               ctx, HandlerExecutionStrategy.SequentialContinueOnError, ct);
         }

         if (!_firstConnect)
         {
            // reset the necessary state fields
            _identifierGenerator.Reset();
            _signalBroker.Reset();
         }

         _clientTokenSource.Dispose();
         _clientTokenSource = new CancellationTokenSource();

         _disconnectReason = null;
         _disconnectException = null;
         _disconnectUserProperties = null;
         _disconnectReasonString = null;

         _firstConnect = false;

         Result<ClientConnectResult, StringError> result;
         if (ct.CanBeCanceled)
         {
            result = await ConnectInternalAsync(ct);
         }
         else
         {
            using var timed = new CancellationTokenSource(_connectOptions.Timeout);
            result = await ConnectInternalAsync(timed.Token);
         }

         if (result.Failed)
         {
            TraceLogger.LogClientError("MqttClient.ConnectAsync: Connection attempt failed: {0}", result.Error.Detail);
            await DisconnectInternalAsync();
            return result;
         }

         var connectResult = result.Success;
         StartKeepAliveOnDemand(connectResult);

         var receiveMax = connectResult.ReceiveMaximum ?? 65535;
         if (receiveMax == 0) receiveMax = 65535;

         _inFlightSemaphore = new SemaphoreSlim(receiveMax, receiveMax);
         lock (_topicAliases)
         {
            _incomingInFlightCount = 0;
         }

         TraceLogger.LogClientInfo("MqttClient.ConnectAsync: Successfully connected. Assigned KeepAlive: {0}s", connectResult.ServerKeepAlive > 0 ? connectResult.ServerKeepAlive : _connectOptions.KeepAlivePeriod);
         CompareExchangeState(MqttClientConnectionState.Connected, MqttClientConnectionState.Connecting);

         await DispatchConnectedAsync(connectResult);
         return result;
      }
      catch (Exception error)
      {
         TraceLogger.LogClientError("MqttClient.ConnectAsync: Unexpected error during connection: {0}", error.Message);
         _disconnectReason = new MqttClientDisconnectReason(false, (int)DisconnectReasonCode.UnspecifiedError);
         _disconnectException = error;

         await DisconnectInternalAsync();

         return new StringError($"Unexpected error at ConnectAsync: {error}");
      }
   }

   internal bool TryDispatch<T>(in T packet, ushort identifier)
   {
      return _signalBroker.TryDispatch(in packet, identifier);
   }

   internal bool TryGetTopicAlias(ushort alias, [NotNullWhen(true)] out byte[]? topic)
   {
      return _topicAliases.TryGetValue(alias, out topic);
   }

   internal void SetTopicAlias(ushort alias, byte[] topic)
   {
      _topicAliases[alias] = topic;
   }

   internal bool TryIncrementIncomingInFlight(ushort receiveMaximum, out int current)
   {
      lock (_topicAliases)
      {
         current = _incomingInFlightCount;
         if (receiveMaximum > 0 && current >= receiveMaximum)
         {
            return false;
         }

         _incomingInFlightCount++;
         return true;
      }
   }

    internal void DecrementIncomingInFlight()
    {
      lock (_topicAliases)
      {
         if (_incomingInFlightCount > 0)
         {
            _incomingInFlightCount--;
         }
      }
   }

   private async Task DispatchConnectedAsync(ClientConnectResult connectResult)
   {
      if (Events.OnClientConnected.Count == 0)
         return;

      var ctx = new ClientConnectedContext()
      {
         ConnectResult = connectResult
      };

      await Events.OnClientConnected.ExecuteAsync(ctx, HandlerExecutionStrategy.SequentialContinueOnError);
   }

   private async Task<Result<ClientConnectResult, StringError>> ConnectInternalAsync(CancellationToken ct = default)
   {
      using var combined = CancellationTokenSource.CreateLinkedTokenSource(ct, _clientTokenSource.Token);
      var connectRes = await _networkClient.ConnectAsync(_connectOptions.EndPoint, combined.Token);

      if (connectRes.Failed)
      {
         return new StringError(connectRes.Error.Message);
      }

      _networkSession = connectRes.Success;
      TraceLogger.LogClientInfo("MqttClient.ConnectInternalAsync: Network client connected successfully.");

      // even under QUIC, first and foremost we have one main control stream
      var streamRes = await _networkSession.OpenStreamAsync(NetworkStreamDirection.Bidirectional, combined.Token);
      if (streamRes.Failed)
      {
         return new StringError(streamRes.Error.Message);
      }

      _controlStream = streamRes.Success;
      TraceLogger.LogClientInfo("MqttClient.ConnectInternalAsync: Opened bidirectional control stream (StreamId: {0}).", _controlStream.StreamId);
      _receiveTask = RunMessageReceive(_controlStream, _clientTokenSource.Token);

      if (_connectOptions.CredentialsProvider is { } credProvider)
      {
         var credsTask = credProvider.GetCredentialsAsync(_connectOptions, combined.Token);
         var creds = credsTask.IsCompletedSuccessfully ? credsTask.Result : await credsTask;

         _connectOptions.UsernameUtf8Bytes = Encoding.UTF8.GetBytes(creds.UserName);
         _connectOptions.PasswordBytes = creds.Password;
      }

      var first = true;
      AuthPacketResult? authResult = null;

      while (true)
      {
         using var iterationCts = CancellationTokenSource.CreateLinkedTokenSource(combined.Token);

         using var connAckAwaiter = _signalBroker.AddAwaitable<ClientConnectResult>(0);
         using var authAwaiter = _signalBroker.AddAwaitable<AuthPacketResult>(0);

         var connAckTask = connAckAwaiter.WaitOneAsync(iterationCts.Token).AsTask();
         var authTask = authAwaiter.WaitOneAsync(iterationCts.Token).AsTask();

         if (first)
         {
            TraceLogger.LogClientInfo("MqttClient.ConnectInternalAsync: Sending CONNECT packet (ClientId: {0})...", Encoding.UTF8.GetString(_connectOptions.ClientIdUtf8Bytes.Span));
            await SendConnect(_controlStream, _connectOptions, combined.Token);
            first = false;
         }
         else
         {
            if (_connectOptions.AuthenticationHandler is { } handler
                && authResult is not null)
            {
               TraceLogger.LogClientInfo("MqttClient.ConnectInternalAsync: Executing authentication handler for incoming AUTH packet...");
               await handler.ExecuteAsync(new MqttAuthContext()
               {
                  AuthPacket = authResult,
                  PacketSender = this,
                  Broker = _signalBroker,
                  ConnAckTask = connAckTask,
                  ReceiveTask = _receiveTask,
                  AuthTask = authTask,
               }, combined.Token);
            }
            else
            {
               return new StringError("Received AUTH packet from server, but no handler is configured.");
            }
         }

         TraceLogger.LogClientInfo("MqttClient.ConnectInternalAsync: Awaiting handshakes (CONNACK or AUTH)...");
         var completedTask = await Task.WhenAny(connAckTask, authTask, _receiveTask);

         if (completedTask == _receiveTask)
         {
            TraceLogger.LogClientError("MqttClient.ConnectInternalAsync: Receiver task exited unexpectedly during handshake.");
            await _receiveTask;
            return new StringError("Connection closed unexpectedly during handshake.");
         }

         if (completedTask == connAckTask)
         {
            var connAckResult = await connAckTask;
            await iterationCts.CancelAsync();

            try
            {
               await authTask;
            }
            catch { /* ignored */ }

            if (_protocolVersion is MqttProtocolVersion.V50)
            {
               TraceLogger.LogClientInfo("MqttClient.ConnectInternalAsync: Received CONNACK (Reason: {0}).", connAckResult.ReasonCode);
               if (connAckResult.ReasonCode is not ConnectReasonCode.Success)
               {
                  return new StringError($"Connection refused: {connAckResult.ReasonCode}");
               }
            }
            else
            {
               TraceLogger.LogClientInfo("MqttClient.ConnectInternalAsync: Received CONNACK (ReturnCode: {0}).", connAckResult.ReturnCode);
               if (connAckResult.ReturnCode is not ConnectReturnCode.Accepted)
               {
                  return new StringError($"Connection refused: {connAckResult.ReturnCode}");
               }
            }

            return connAckResult;
         }

         authResult = await authTask;
         await iterationCts.CancelAsync();

         try
         {
            await connAckTask;
         }
         catch { /* ignored */ }

         TraceLogger.LogClientInfo("MqttClient.ConnectInternalAsync: Received AUTH packet.");
         if (_connectOptions.AuthenticationMethodUtf8Bytes.IsEmpty)
         {
            return new StringError("Received AUTH packet from server, but no authentication method is configured.");
         }
      }
   }

   internal ValueTask HandleDisconnect(DisconnectPacket packet, CancellationToken ct = default)
   {
      TraceLogger.LogClientWarning("MqttClient: Received DISCONNECT from server (ReasonCode: {0}).", packet.ReasonCode);
      _disconnectReason = new MqttClientDisconnectReason(false, (int)packet.ReasonCode);

      return DisconnectInternalAsync(false);
   }

   private void StartKeepAliveOnDemand(ClientConnectResult connectResult)
   {
      var keepAliveInterval = _connectOptions.KeepAlivePeriod;

      if (connectResult.ServerKeepAlive is > 0 and var serverKeepAlive)
      {
         keepAliveInterval = serverKeepAlive;
      }

      var timeSpan = TimeSpan.FromSeconds(keepAliveInterval);
      if (timeSpan.TotalSeconds > 0)
      {
         _keepAliveTask = RunKeepAliveTask(timeSpan, _clientTokenSource.Token);
      }
   }

   private VoidResult<StringError> ValidateClient()
   {
      var disposed = ValidateDisposed();
      if (disposed.Failed)
         return disposed;

      if ((MqttClientConnectionState)_state is not MqttClientConnectionState.Connected)
         return new StringError("Client is not connected.");

      return true;
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   private VoidResult<StringError> ValidateDisposed()
   {
      if (_disposed)
         return new StringError("Client is already disposed.");

      return true;
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   private MqttClientConnectionState CompareExchangeState(
      MqttClientConnectionState state, MqttClientConnectionState compareState)
   {
      return (MqttClientConnectionState)Interlocked.CompareExchange(
         ref _state, (int)state, (int)compareState);
   }

   public async ValueTask DisposeAsync()
   {
      if (_disposed) return;
      _disposed = true;

      _signalBroker.Dispose();
      _clientTokenSource.Dispose();

      await _networkClient.DisposeAsync();
   }
}
