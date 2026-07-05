using Beskar.Memory.Threading;
using Beskar.Mqtt.Common.Handlers.Contexts;

namespace Beskar.Mqtt.Common.Handlers.Client;

public sealed class ClientEvents
{
   public readonly HandlerPipeline<MessageReceiveContext> OnMessageReceive = new();
}
