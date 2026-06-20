using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Extensions;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Parsers.Version3;

public readonly ref partial struct PacketVersion3Parser
{
   public Result<PacketDispatchResult, StringError> TryParseConnectPacket(
      ref RawPacket rawPacket,
      ref ConnectPacket packet)
   {
      // protocol name and version, try private flag is already parsed at this point
      if (!rawPacket.Reader.TryRead(out var connectFlags))
      {
         return new StringError("Could not read connect flags.");
      }

      if ((connectFlags & 0x1) > 0)
      {
         return new StringError("First bit is not 0 set.");
      }

      packet.IsCleanSession = (connectFlags & 0x2) > 0;

      var willFlag = (connectFlags & 0x4) > 0;
      var willQoS = (connectFlags & 0x18) >> 3;
      var willRetain = (connectFlags & 0x20) > 0;
      var passwordFlag = (connectFlags & 0x40) > 0;
      var usernameFlag = (connectFlags & 0x80) > 0;

      if (!rawPacket.Reader.TryReadUInt16BigEndian(out packet.KeepAliveInterval))
      {
         return new StringError("Could not read keep alive interval.");
      }


   }
}
