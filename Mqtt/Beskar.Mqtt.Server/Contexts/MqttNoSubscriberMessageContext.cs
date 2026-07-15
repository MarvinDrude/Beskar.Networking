using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttNoSubscriberMessageContext
{
   public required MqttSession Session { get; init; }

   public required PublishPacket PublishPacket { get; init; }
}
