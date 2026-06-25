using System.Buffers;
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

public sealed partial class MqttClient : IMqttClient
{
   public bool IsConnected => (MqttClientConnectionState)_state is MqttClientConnectionState.Connected;

   private readonly INetworkClient _networkClient;
   private readonly IPacketHandler _packetHandler = null!;

   private volatile bool _disposed;
   private volatile int _state = (int)MqttClientConnectionState.Disconnected;

   internal MqttClient(INetworkClient client)
   {
      _networkClient = client;
   }

   public Task<ClientConnectResult> ConnectAsync(
      ConnectOptions options, CancellationToken ct = default)
   {


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

   public Task<VoidResult<StringError>> PingAsync(CancellationToken ct = default)
   {
      throw new NotImplementedException();
   }

   private VoidResult<StringError> ValidateClient()
   {
      if (_disposed)
         return new StringError("Client is already disposed.");

      if (_state is not MqttClientConnectionState.Connected)
         return new StringError("Client is not connected.");

      return true;
   }

   public ValueTask DisposeAsync()
   {
      if (_disposed) return ValueTask.CompletedTask;
      _disposed = true;

      return ValueTask.CompletedTask;
   }
}
