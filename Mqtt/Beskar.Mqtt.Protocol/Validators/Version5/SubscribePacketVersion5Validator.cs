using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Validators.Common;

namespace Beskar.Mqtt.Protocol.Validators.Version5;

public static class SubscribePacketVersion5Validator
{
   public static VoidResult<StringError> Validate(ref SubscribePacket packet)
      => PacketFilterValidator.ValidateSubscribe(ref packet, isVersion3: false);
}
