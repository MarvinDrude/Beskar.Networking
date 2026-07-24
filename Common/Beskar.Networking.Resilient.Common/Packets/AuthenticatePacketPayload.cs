using Beskar.Networking.Resilient.Common.Interfaces;

namespace Beskar.Networking.Resilient.Common.Packets;

public sealed class AuthenticatePacketPayload : IResilientPayload
{
   public string? AuthMethod { get; set; }

   public byte[]? AuthData { get; set; }
}
