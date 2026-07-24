using Beskar.Networking.Resilient.Common.Interfaces;

namespace Beskar.Networking.Resilient.Common.Packets;

public sealed class DisconnectPacketPayload : IResilientPayload
{
   public byte ReasonCode { get; set; }

   public string? ReasonString { get; set; }
}
