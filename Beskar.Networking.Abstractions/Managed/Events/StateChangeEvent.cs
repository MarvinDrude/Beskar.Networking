namespace Beskar.Networking.Abstractions.Managed.Events;

public readonly struct StateChangeEvent(
   NetworkClient client,
   ConnectionState from,
   ConnectionState to)
{
   public readonly NetworkClient Client = client;

   public readonly ConnectionState From = from;
   public readonly ConnectionState To = to;
}
