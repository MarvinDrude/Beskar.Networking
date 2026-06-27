using System.Buffers;
using System.Runtime.CompilerServices;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Client.States;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Parsers;
using Beskar.Mqtt.Common.Results;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Client;

public sealed partial class MqttClient(INetworkClient client) : IMqttClient
{
   public bool IsConnected => (MqttClientConnectionState)_state is MqttClientConnectionState.Connected;

   private readonly INetworkClient _networkClient = client;
   private INetworkSession _controlSession;

   private readonly IPacketHandler _packetHandler = null!;

   private volatile bool _disposed;
   private volatile int _state = (int)MqttClientConnectionState.Disconnected;

   private ConnectOptions _connectOptions = new ();

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
         _connectOptions = options;
         _networkClient.
      }
      catch (Exception error)
      {
         return new StringError($"Unexpected error at ConnectAsync: {error}");
      }

      return Task.FromResult(new ClientConnectResult());
   }

   public Task<Result<PublishResult, StringError>> PublishAsync(
      PublishOptions options, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public async Task<Result<SubscribeResult, StringError>> SubscribeAsync(
      SubscribeOptions options, CancellationToken ct = default)
   {
      var validateResult = SubscribeOptionsValidator.Validate(options);
      if (!validateResult.IsSuccess) return validateResult.Error;

      var clientResult = ValidateClient();
      if (!clientResult.IsSuccess) return clientResult.Error;


   }

   public Task<Result<UnsubscribeResult, StringError>> UnsubscribeAsync(
      UnsubscribeOptions options, CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   public async Task<VoidResult<StringError>> PingAsync(CancellationToken ct = default)
   {
      var clientResult = ValidateClient();
      if (!clientResult.IsSuccess) return clientResult;


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

      return ValueTask.CompletedTask;
   }
}
