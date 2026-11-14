using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Validators.Common;

namespace Beskar.Mqtt.Protocol.Validators.Version3;

public static class UnsubscribePacketVersion3Validator
{
   public static VoidResult<StringError> Validate(ref UnsubscribePacket packet)
      => PacketFilterValidator.ValidateUnsubscribe(ref packet);
}
