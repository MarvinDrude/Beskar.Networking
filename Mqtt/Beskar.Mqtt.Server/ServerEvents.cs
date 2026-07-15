using Beskar.Memory.Threading;
using Beskar.Mqtt.Server.Contexts;

namespace Beskar.Mqtt.Server;

/// <summary>
/// Container for all MQTT server events and hook pipelines.
/// </summary>
public sealed class ServerEvents
{
   /// <summary>
   /// Pipeline fired when an incoming client connection is initiated but before session creation or negotiation.
   /// Called in MqttServer client connection loop upon receiving the CONNECT packet.
   /// Allows intercepting AUTH challenges, modifying connection parameters, or rejecting the connection.
   /// </summary>
   public readonly HandlerPipeline<MqttConnectInterceptContext> OnConnectIntercept = new();

   /// <summary>
   /// Pipeline fired when a new session is being created for a client.
   /// Called inside the session manager (MqttClientSessions.GetOrCreateSessionAsync) when no matching persistent session exists
   /// or when the client requests a clean start.
   /// </summary>
   public readonly HandlerPipeline<MqttNewSessionContext> OnNewSession = new();

   /// <summary>
   /// Pipeline fired when a client has successfully completed its connection handshake and a CONNACK has been sent.
   /// Called on the client's connection thread.
   /// </summary>
   public readonly HandlerPipeline<MqttConnectContext> OnConnect = new();

   /// <summary>
   /// Pipeline fired when a client session terminates (due to a clean disconnect or connection loss/timeout).
   /// Called on the connection cleanup path.
   /// </summary>
   public readonly HandlerPipeline<MqttDisconnectContext> OnDisconnect = new();

   /// <summary>
   /// Pipeline fired after the server has successfully started and bound its listeners.
   /// Called at the end of MqttServer.StartAsync once state transitions to Running.
   /// </summary>
   public readonly HandlerPipeline<MqttServerStartContext> OnStart = new();

   /// <summary>
   /// Pipeline fired after the server has stopped and unbound its listeners.
   /// Called at the end of MqttServer.StopAsync once state transitions to Stopped.
   /// </summary>
   public readonly HandlerPipeline<MqttServerStopContext> OnStop = new();

   /// <summary>
   /// Pipeline fired when a client session is permanently deleted/disposed from the server.
   /// Called inside MqttSession.DisposeAsync due to disconnect session teardown, session takeover, or session expiry.
   /// </summary>
   public readonly HandlerPipeline<MqttDeleteSessionContext> OnDeleteSession = new();

   /// <summary>
   /// Pipeline fired when a client successfully subscribes to a topic filter.
   /// Called in MqttServer.Subscribe after registering the filter and before delivering matching retained messages.
   /// </summary>
   public readonly HandlerPipeline<MqttSubscribeContext> OnSubscribe = new();

   /// <summary>
   /// Pipeline fired when a client unsubscribes from a topic filter.
   /// Called in MqttServer.Unsubscribe after removing the filter from the subscription router.
   /// </summary>
   public readonly HandlerPipeline<MqttUnsubscribeContext> OnUnsubscribe = new();

   /// <summary>
   /// Pipeline fired when the server acknowledges a message published by a client.
   /// Called in ServerPacketHandler.Publish.cs for QoS 1 (PUBACK) and QoS 2 (PUBREC) publishes when replying to the publisher.
   /// </summary>
   public readonly HandlerPipeline<MqttAcknowledgePubContext> OnAcknowledgePub = new();

   /// <summary>
   /// Pipeline fired when a client publishes a message that matches zero active subscriptions.
   /// Called inside ServerPacketHandler.Publish.cs after subscription routing yields no matches.
   /// </summary>
   public readonly HandlerPipeline<MqttNoSubscriberMessageContext> OnNoSubscriberMessage = new();

   /// <summary>
   /// Pipeline fired when a client acknowledges a QoS 1 or QoS 2 message published by the server.
   /// Called inside ServerPacketHandler.cs in PubAck (QoS 1) and PubComp (QoS 2) packet handlers when the client completes the handshake.
   /// </summary>
   public readonly HandlerPipeline<MqttPublishAcknowledgedContext> OnPublishAcknowledged = new();

   /// <summary>
   /// Pipeline fired when a retained message is stored, updated, or deleted from the server cache.
   /// Called in ServerPacketHandler.Publish.cs when processing a publish packet with Retain = true.
   /// </summary>
   public readonly HandlerPipeline<MqttRetainedMessageChangedContext> OnRetainedMessageChanged = new();

   /// <summary>
   /// Pipeline fired at server startup to allow the host application to seed/load retained messages from persistent storage.
   /// Called inside MqttServer.StartAsync immediately after state transitions to Starting.
   /// </summary>
   public readonly HandlerPipeline<MqttLoadingRetainedMessagesContext> OnLoadingRetainedMessages = new();

   /// <summary>
   /// Pipeline fired when all retained messages are cleared from the server.
   /// Called inside MqttServer.ClearRetainedMessagesAsync.
   /// </summary>
   public readonly HandlerPipeline<MqttRetainedMessagesClearedContext> OnRetainedMessagesCleared = new();
}
