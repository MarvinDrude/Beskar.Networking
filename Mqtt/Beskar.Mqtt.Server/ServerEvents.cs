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

   public readonly HandlerPipeline<MqttSubscribeContext> OnSubscribe = new();

   public readonly HandlerPipeline<MqttUnsubscribeContext> OnUnsubscribe = new();

   public readonly HandlerPipeline<MqttAcknowledgePubContext> OnAcknowledgePub = new();

   public readonly HandlerPipeline<MqttNoSubscriberMessageContext> OnNoSubscriberMessage = new();

   public readonly HandlerPipeline<MqttPublishAcknowledgedContext> OnPublishAcknowledged = new();

   public readonly HandlerPipeline<MqttRetainedMessageChangedContext> OnRetainedMessageChanged = new();

   public readonly HandlerPipeline<MqttLoadingRetainedMessagesContext> OnLoadingRetainedMessages = new();

   public readonly HandlerPipeline<MqttRetainedMessagesClearedContext> OnRetainedMessagesCleared = new();
}
