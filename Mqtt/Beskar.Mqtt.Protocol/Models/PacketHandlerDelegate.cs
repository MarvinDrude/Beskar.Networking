namespace Beskar.Mqtt.Protocol.Models;

public delegate ValueTask PacketHandlerFunc<TPacket>(in TPacket packet, CancellationToken ct = default)
   where TPacket : struct, allows ref struct;
