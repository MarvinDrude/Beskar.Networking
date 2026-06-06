using Beskar.Networking.Abstractions.Errors;

namespace Beskar.Networking.Abstractions.Managed.Events;

public readonly struct ConnectionFailedEvent(
   NetworkClient client,
   NetworkCodeError error)
{
   public readonly NetworkClient Client = client;
   public readonly NetworkCodeError Error = error;
}
