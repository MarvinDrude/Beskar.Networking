using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Handlers.Contexts;
using Beskar.Mqtt.Common.Models;
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
   /// Sends a PUBLISH request packet and awaits the subscriber's response matching ResponseTopic and CorrelationData.
   /// </summary>
   /// <param name="options">The publish options for the request.</param>
   /// <param name="timeout">Maximum time to wait for a response before timing out. Defaults to 10 seconds if default.</param>
   /// <param name="ct">Cancellation token.</param>
   public Task<Result<MqttResponseContext, StringError>> RequestAsync(
      PublishOptions options, TimeSpan timeout = default, CancellationToken ct = default);

   /// <summary>
   /// Sends a PUBLISH request packet to the specified topic and awaits the subscriber's response.
   /// </summary>
   /// <param name="topic">The target topic to publish to.</param>
   /// <param name="payload">The request payload.</param>
   /// <param name="timeout">Maximum time to wait for a response before timing out. Defaults to 10 seconds if default.</param>
   /// <param name="ct">Cancellation token.</param>
   public Task<Result<MqttResponseContext, StringError>> RequestAsync(
      string topic, ReadOnlyMemory<byte> payload, TimeSpan timeout = default, CancellationToken ct = default);

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
   /// Adds a message receive handler called for all incoming published messages
   /// that match the ones that you subscribed to.
   /// </summary>
   /// <returns>Returns a disposable that when disposed, removes the message receive handler.</returns>
   public IDisposable AddMessageReceiveHandler(
      Func<MessageReceiveContext, CancellationToken, ValueTask> messageReceiveHandler);

   /// <summary>
   /// Registers a handler to be invoked when the MQTT client begins the connection process.
   /// This event is triggered before establishing an actual connection with the server.
   /// </summary>
   /// <param name="handler">
   /// A function that receives a <see cref="ClientConnectingContext"/> and a <see cref="CancellationToken"/>.
   /// The handler is awaited and can perform any necessary logic before the connection is established.
   /// </param>
   /// <returns>
   /// A disposable object that, when disposed, removes the registered connecting handler.
   /// </returns>
   public IDisposable AddConnectingHandler(
      Func<ClientConnectingContext, CancellationToken, ValueTask> handler);

   /// <summary>
   /// Adds a handler invoked when the client has successfully connected to the MQTT server.
   /// This allows users to define custom actions that should occur after a successful connection.
   /// </summary>
   /// <param name="handler">A function to handle events related to the client being successfully connected.
   /// The function takes a <see cref="ClientConnectedContext"/> and a <see cref="CancellationToken"/> as parameters
   /// and returns a <see cref="ValueTask"/>.</param>
   /// <returns>An <see cref="IDisposable"/> instance that can be used to remove the added handler.</returns>
   public IDisposable AddConnectedHandler(
      Func<ClientConnectedContext, CancellationToken, ValueTask> handler);

   /// <summary>
   /// Adds a handler invoked when the MQTT client disconnects.
   /// </summary>
   /// <param name="handler">
   /// A delegate representing the handler to be invoked. The handler takes a <see cref="ClientDisconnectedContext"/>
   /// that provides information about the disconnection event and a <see cref="CancellationToken"/> to handle asynchronous operations.
   /// </param>
   /// <returns>An <see cref="IDisposable"/> token that can be used to remove the handler.</returns>
   public IDisposable AddDisconnectedHandler(
      Func<ClientDisconnectedContext, CancellationToken, ValueTask> handler);
}
