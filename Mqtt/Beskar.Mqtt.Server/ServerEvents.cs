using Beskar.Memory.Threading;
using Beskar.Mqtt.Server.Contexts;

namespace Beskar.Mqtt.Server;

public sealed class ServerEvents
{
   public readonly HandlerPipeline<MqttConnectInterceptContext> OnConnectIntercept = new();
}
