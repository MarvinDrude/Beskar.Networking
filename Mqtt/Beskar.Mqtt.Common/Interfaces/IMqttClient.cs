using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Protocol.Results;

namespace Beskar.Mqtt.Common.Interfaces;

/// <summary>
/// Represents a full MQTT Client able to SUBSCRIBE, UNSUBSCRIBE, PUBLISH, PING.
/// <remarks>Connection states etc. are not fully thread safe.</remarks>
/// <remarks>PUBLISH, SUBSCRIBE, UNSUBSCRIBE, PING are fully thread safe to call.</remarks>
/// </summary>
public interface IMqttClient : IAsyncDisposable
{
   /// <summary>
   /// Whether the MQTT Client is currently connected to the server last
   /// specified in the Connect Method.
   /// </summary>
   public bool IsConnected { get; }

   /// <summary>
   /// Starts trying to connect to a MQTT server with the options provided.
   /// Underlying networking options are provided in the creation factory of the client.
   /// </summary>
   public Task<Result<ClientConnectResult, StringError>> ConnectAsync(
      ConnectOptions options, CancellationToken ct = default);

   /// <summary>
   /// Tries to disconnect from the MQTT server if not already in process
   /// or
   /// </summary>
   public Task DisconnectAsync(
      DisconnectOptions options, CancellationToken ct = default);

   /// <summary>
   /// Send a new PUBLISH Packet given the input options.
   /// </summary>
   public Task<Result<PublishResult, StringError>> PublishAsync(
      PublishOptions options, CancellationToken ct = default);

   /// <summary>
   /// Send a new SUBSCRIBE Packet given the input options.
   /// </summary>
   public Task<Result<SubscribeResult, StringError>> SubscribeAsync(
      SubscribeOptions options, CancellationToken ct = default);

   /// <summary>
   /// Send a new UNSUBSCRIBE Packet given the input options.
   /// </summary>
   public Task<Result<UnsubscribeResult, StringError>> UnsubscribeAsync(
      UnsubscribeOptions options, CancellationToken ct = default);

   /// <summary>
   /// Send a new PING packet.
   /// </summary>
   public Task<VoidResult<StringError>> PingAsync(CancellationToken ct = default);

   /// <summary>
   /// Adds a message receive handler that is called for all incoming published messages
   /// that match the ones that you subscribed to.
   /// </summary>
   /// <returns>Returns a disposable that when disposed, removes the message receive handler.</returns>
   public IDisposable AddMessageReceiveHandler(
      Func<MessageReceiveContext, CancellationToken, ValueTask> messageReceiveHandler);
}
