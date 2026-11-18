namespace Beskar.Networking.Datagram.Enums;

/// <summary>
/// Controls how incoming events are surfaced by <see cref="DatagramHost"/>.
/// </summary>
public enum DatagramEventDispatchMode : byte
{
   /// <summary>
   /// Events are queued for zero-allocation synchronous consumption via <see cref="DatagramHost.TryPollEvent"/>.
   /// </summary>
   Poll = 0,

   /// <summary>
   /// Events are dispatched via background async readers and <see cref="DatagramHost.Events"/> delegates.
   /// </summary>
   Async = 1
}
