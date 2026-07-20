namespace Beskar.Networking.Transports.Memory;

/// <summary>
/// Represents options for configuring an in-memory transport.
/// </summary>
public sealed class MemoryTransportOptions
{
   /// <summary>
   /// The maximum number of pending connections that can be queued in the listener's session channel.
   /// Defaults to 1024.
   /// </summary>
   public int MaxPendingConnections { get; set; } = 1024;
}
