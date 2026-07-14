using Beskar.Memory.Threading;
using Beskar.Mqtt.Common.Handlers.Contexts;

namespace Beskar.Mqtt.Common.Handlers.Client;

public sealed class ClientEvents
{
   public readonly HandlerPipeline<MessageReceiveContext> OnMessageReceive = new();

   public readonly HandlerPipeline<ClientConnectingContext> OnClientConnecting = new();

   public readonly HandlerPipeline<ClientConnectedContext> OnClientConnected = new();

   public readonly HandlerPipeline<ClientDisconnectedContext> OnClientDisconnected = new();
}
