using Beskar.Networking.Resilient.Common.Interfaces;

namespace Beskar.Networking.Resilient.Common.Packets;

public sealed class ConnectPacketPayload : IResilientPayload
{
   public ushort KeepAliveSeconds { get; set; }
}
