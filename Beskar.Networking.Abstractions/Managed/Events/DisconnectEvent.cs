namespace Beskar.Networking.Abstractions.Managed.Events;

public readonly struct DisconnectEvent(NetworkClient client)
{
   public readonly NetworkClient Client = client;
}
