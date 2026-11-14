using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Validators.Common;

namespace Beskar.Mqtt.Protocol.Validators.Version5;

public static class UnsubscribePacketVersion5Validator
{
   public static VoidResult<StringError> Validate(ref UnsubscribePacket packet)
      => PacketFilterValidator.ValidateUnsubscribe(ref packet);
}
