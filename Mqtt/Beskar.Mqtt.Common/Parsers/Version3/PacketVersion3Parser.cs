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

   public ValueTask<PacketDispatchResult> TryDispatch(
      ref RawPacket rawPacket,
      out int bytesConsumed,
      CancellationToken cancellation = default)
   {
      bytesConsumed = 0;

      var packetType = rawPacket.FixedHeader >> 4;
      if (packetType is < 1 or >= 15)
      {
         return ValueTask.FromResult(PacketDispatchResult.InvalidPacketType);
      }

      switch ((MqttPacketType)packetType)
      {
         case MqttPacketType.Connect:
            var packet = new ConnectPacket();
            var result = TryParseConnectPacket(ref rawPacket, ref packet);

            if (result.Failed)
            {
               TraceLogger.LogNeutralError("Error at parsing ConnectPacket: {0}", result.Error.Detail);
               return ValueTask.FromResult(PacketDispatchResult.ProtocolError);
            }

            bytesConsumed = rawPacket.TotalLength;
            var valueTask = _packetHandler.ExecuteAsync(in packet, cancellation);

            return valueTask.IsCompletedSuccessfully
               ? new ValueTask<PacketDispatchResult>(PacketDispatchResult.Success)
               : AwaitHandler(valueTask);
      }

      return ValueTask.FromResult(PacketDispatchResult.InvalidPacketType);
   }

   private static async ValueTask<PacketDispatchResult> AwaitHandler(ValueTask task)
   {
      await task;
      return PacketDispatchResult.Success;
   }
}
