using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Abstractions.Managed.Events;

public readonly struct ConnectEvent(NetworkClient client, INetworkSession session)
{
   public readonly NetworkClient Client = client;
   public readonly INetworkSession Session = session;
}
