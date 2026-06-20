using System.Runtime.InteropServices;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Parsers.Version5;

[StructLayout(LayoutKind.Auto)]
public readonly ref struct PacketVersion5Parser(IPacketHandler handler)
{
   private readonly IPacketHandler _packetHandler = handler;

   public PacketDispatchResult TryDispatch(
      ref RawPacket rawPacket,
      out int bytesConsumed)
   {
      
   }
}
