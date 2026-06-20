namespace Beskar.Mqtt.Protocol.Parsing.Results;

public enum PacketDispatchResult : byte
{
   Success = 1,
   NotEnoughData = 2,
   ProtocolError = 3,
   InvalidPacketType = 4,
}
