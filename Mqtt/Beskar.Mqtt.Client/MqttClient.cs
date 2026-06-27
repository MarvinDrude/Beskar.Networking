using System.Buffers;
using System.Net;
using System.Runtime.CompilerServices;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Client.Handlers;
using Beskar.Mqtt.Client.States;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Generators;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Parsers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Mqtt.Protocol.Results;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient : IMqttClient
{
   public bool IsConnected => (MqttClientConnectionState)_state is MqttClientConnectionState.Connected;

   private readonly INetworkClient _networkClient;
   private INetworkSession? _controlSession;

   private readonly IPacketHandler _packetHandler;
   private readonly SignalBroker _signalBroker = new();
   private readonly PacketIdentifierGenerator _identifierGenerator = new();

   private volatile bool _disposed;
   private volatile bool _gracefulDisconnect;
   private volatile bool _firstConnect = true;
   private volatile int _state = (int)MqttClientConnectionState.Disconnected;

   private CancellationTokenSource _clientTokenSource = new();

   private Task? _keepAliveTask;
   private DateTimeOffset _lastKeepAliveTimestamp;

   private ConnectOptions _connectOptions = new() { EndPoint = new IPEndPoint(0, 0) };

   public MqttClient(INetworkClient networkClient)
   {
      _networkClient = networkClient;
      _packetHandler = new ClientPacketHandler(this);
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

   private async Task<Result<ClientConnectResult, StringError>> ConnectInternalAsync(CancellationToken ct = default)
   {
      using var combined = CancellationTokenSource.CreateLinkedTokenSource(ct, _clientTokenSource.Token);
      var connectRes = await _networkClient.ConnectAsync(_connectOptions.EndPoint, combined.Token);

      if (connectRes.Failed)
      {
         return new StringError(connectRes.Error.Message);
      }



      throw new NotImplementedException();

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

   public ValueTask DisposeAsync()
   {
      if (_disposed) return ValueTask.CompletedTask;
      _disposed = true;

      _signalBroker.Dispose();
      _clientTokenSource.Dispose();

      return ValueTask.CompletedTask;
   }
}
