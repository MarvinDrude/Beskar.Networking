namespace Beskar.Networking.Abstractions.Models;

/// <summary>
/// Represents operational statistics for a network listener.
/// </summary>
public struct NetworkListenerStats
{
   public long Binds { get; set; }

   public long Unbinds { get; set; }

   public long SessionsAccepted { get; set; }
}
