using System.Runtime.InteropServices;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Common.Parsers.Version3;

[StructLayout(LayoutKind.Auto)]
public readonly ref partial struct PacketVersion3Parser(IPacketHandler handler)
{
   private readonly IPacketHandler _packetHandler = handler;

   public PacketDispatchResult TryDispatch(
      ref RawPacket rawPacket,
      out int bytesConsumed)
   {
      bytesConsumed = 0;

      var packetType = rawPacket.FixedHeader >> 4;
      if (packetType is < 1 or >= 15)
      {
         return PacketDispatchResult.InvalidPacketType;
      }

      switch ((MqttPacketType)packetType)
      {
         case MqttPacketType.Connect:
            var packet = new ConnectPacket();
            var result = TryParseConnectPacket(ref rawPacket, ref packet);

            if (result.Failed)
            {
               TraceLogger.LogNeutralError("Error at parsing ConnectPacket: {0}", result.Error.Detail);
               return PacketDispatchResult.ProtocolError;
            }

            _packetHandler.ExecuteAsync(in packet);
            break;
      }
   }
}
