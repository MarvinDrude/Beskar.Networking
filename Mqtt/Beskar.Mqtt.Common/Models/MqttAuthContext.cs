using Beskar.Mqtt.Protocol.Results;

namespace Beskar.Mqtt.Common.Models;

public sealed class MqttAuthContext
{
   public required AuthPacketResult AuthPacket { get; init; }
}
