using Beskar.Memory.Threading;
using Beskar.Mqtt.Common.Handlers.Contexts;

namespace Beskar.Mqtt.Common.Handlers.Client;

/// <summary>
/// Represents a collection of events that can be subscribed to and handled during the lifecycle of an MQTT client.
/// Provides event pipelines for message reception and connection lifecycle events.
/// </summary>
public sealed class ClientEvents
{
   /// <summary>
   /// Event pipeline triggered when the MQTT client receives a new publish message.
   /// Handlers registered to this event will be invoked with a <see cref="MessageReceiveContext"/>,
   /// which provides details about the received message and the ability to acknowledge or respond to it.
   /// </summary>
   public readonly HandlerPipeline<MessageReceiveContext> OnMessageReceive = new();

   /// <summary>
   /// Event pipeline triggered when the MQTT client begins the process of connecting to the broker.
   /// Handlers registered to this event will be invoked with a <see cref="ClientConnectingContext"/>,
   /// which provides details about the connection parameters and allows for pre-connection logic or modifications.
   /// </summary>
   public readonly HandlerPipeline<ClientConnectingContext> OnClientConnecting = new();

   /// <summary>
   /// Event pipeline triggered when the MQTT client successfully establishes a connection to the broker.
   /// Handlers registered to this event will be invoked with a <see cref="ClientConnectedContext"/>,
   /// which provides information about the established connection and allows for post-connection logic to execute.
   /// </summary>
   public readonly HandlerPipeline<ClientConnectedContext> OnClientConnected = new();

   /// <summary>
   /// Event pipeline triggered when an MQTT client disconnects from the server.
   /// Handlers registered to this event will be executed with a <see cref="ClientDisconnectedContext"/>,
   /// providing context about the disconnection, including the reason and additional metadata.
   /// </summary>
   public readonly HandlerPipeline<ClientDisconnectedContext> OnClientDisconnected = new();
}
