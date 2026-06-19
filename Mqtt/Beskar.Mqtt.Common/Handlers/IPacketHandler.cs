using Beskar.Mqtt.Common.Handlers.Interfaces;

namespace Beskar.Mqtt.Common.Handlers;

public interface IPacketHandler
   : IAuthHandler,
   IConnAckHandler,
   IConnectHandler,
   IDisconnectHandler,
   IPingReqHandler,
   IPingRespHandler,
   IPubAckHandler,
   IPubCompHandler,
   IPublishHandler,
   IPubRecHandler,
   IPubRelHandler,
   ISubAckHandler,
   ISubscribeHandler,
   IUnsubAckHandler,
   IUnsubscribeHandler;
