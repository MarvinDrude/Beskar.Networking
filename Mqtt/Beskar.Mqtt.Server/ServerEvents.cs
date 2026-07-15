using Beskar.Memory.Threading;
using Beskar.Mqtt.Server.Contexts;

namespace Beskar.Mqtt.Server;

public sealed class ServerEvents
{
   public readonly HandlerPipeline<MqttConnectInterceptContext> OnConnectIntercept = new();

   public readonly HandlerPipeline<MqttNewSessionContext> OnNewSession = new();

   public readonly HandlerPipeline<MqttConnectContext> OnConnect = new();

   public readonly HandlerPipeline<MqttDisconnectContext> OnDisconnect = new();

   public readonly HandlerPipeline<MqttServerStartContext> OnStart = new();

   public readonly HandlerPipeline<MqttServerStopContext> OnStop = new();

   public readonly HandlerPipeline<MqttDeleteSessionContext> OnDeleteSession = new();
}
