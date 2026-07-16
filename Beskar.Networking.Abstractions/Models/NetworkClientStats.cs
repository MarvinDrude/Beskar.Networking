namespace Beskar.Networking.Abstractions.Models;

/// <summary>
/// Represents operational statistics for a network client.
/// </summary>
public struct NetworkClientStats
{
   public long ConnectionsEstablished { get; set; }

   public long ConnectionsLost { get; set; }
}
