namespace Beskar.Networking.Abstractions.Managed.Events;

public readonly struct ReconnectEvent(
   NetworkClient client,
   int attempt,
   TimeSpan delay)
{
   public readonly NetworkClient Client = client;

   public readonly int Attempt = attempt;
   public readonly TimeSpan Delay = delay;
}
