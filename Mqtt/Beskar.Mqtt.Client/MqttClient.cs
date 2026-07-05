using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Client.Handlers;
using Beskar.Mqtt.Client.States;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Generators;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Common.Handlers.Client;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Models;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Results;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient : IMqttClient
{
   public bool IsConnected => (MqttClientConnectionState)_state is MqttClientConnectionState.Connected;
   public ClientEvents Events { get; } = new();

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

   private MqttProtocolVersion _protocolVersion = MqttProtocolVersion.Unknown;

   private CancellationTokenSource _clientTokenSource = new();

   private Task? _receiveTask;
   private Task? _keepAliveTask;
   private DateTimeOffset _lastKeepAliveTimestamp;

   private ConnectOptions _connectOptions = new() { EndPoint = new IPEndPoint(0, 0) };

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

      try
      {
         _protocolVersion = options.ProtocolVersion;

         if (!_firstConnect)
         {
            // reset the necessary state fields
            _identifierGenerator.Reset();
            _signalBroker.Reset();
         }

         _clientTokenSource.Dispose();
         _clientTokenSource = new CancellationTokenSource();

         _connectOptions = options;
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
            await DisconnectInternalAsync();
            return result;
         }

         var connectResult = result.Success;
         StartKeepAliveOnDemand(connectResult);

         CompareExchangeState(MqttClientConnectionState.Connected, MqttClientConnectionState.Connecting);
         return result;
      }
      catch (Exception error)
      {
         return new StringError($"Unexpected error at ConnectAsync: {error}");
      }
   }


   internal bool TryDispatch<T>(in T packet, ushort identifier)
   {
      return _signalBroker.TryDispatch(in packet, identifier);
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

      // even under QUIC, first and foremost we have one main control stream
      var streamRes = await _networkSession.AcceptStreamAsync(combined.Token);
      if (streamRes.Failed)
      {
         return new StringError(streamRes.Error.Message);
      }

      _controlStream = streamRes.Success;
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
         using var connAckAwaiter = _signalBroker.AddAwaitable<ClientConnectResult>(0);
         using var authAwaiter = _signalBroker.AddAwaitable<AuthPacketResult>(0);

         if (first)
         {
            await SendConnect(_controlStream, _connectOptions, combined.Token);
            first = false;
         }
         else
         {
            if (_connectOptions.AuthenticationHandler is { } handler
                && authResult is not null)
            {
               await handler.ExecuteAsync(new MqttAuthContext()
               {
                  AuthPacket = authResult
               }, combined.Token);
            }
            else
            {
               return new StringError("Received AUTH packet from server, but no handler is configured.");
            }
         }

         var connAckTask = connAckAwaiter.WaitOneAsync(combined.Token).AsTask();
         var authTask = authAwaiter.WaitOneAsync(combined.Token).AsTask();

         var completedTask = await Task.WhenAny(connAckTask, authTask, _receiveTask);

         if (completedTask == _receiveTask)
         {
            await _receiveTask;
            return new StringError("Connection closed unexpectedly during handshake.");
         }

         if (completedTask == connAckTask)
         {
            var connAckResult = await connAckTask;
            if (connAckResult.ReasonCode != ConnectReasonCode.Success)
            {
               return new StringError($"Connection refused: {connAckResult.ReasonCode}");
            }

            return connAckResult;
         }

         authResult = await authTask;
         if (_connectOptions.AuthenticationMethodUtf8Bytes.IsEmpty)
         {
            return new StringError("Received AUTH packet from server, but no authentication method is configured.");
         }
      }
   }

   internal ValueTask HandleDisconnect(DisconnectPacket packet, CancellationToken ct = default)
   {
      _disconnectReason = new MqttClientDisconnectReason(false, (int)packet.ReasonCode);
      return DisconnectInternalAsync();
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
