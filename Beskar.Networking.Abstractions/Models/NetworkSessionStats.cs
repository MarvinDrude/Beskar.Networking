namespace Beskar.Networking.Abstractions.Models;

/// <summary>
/// Represents stream statistics for a network session.
/// </summary>
public struct NetworkSessionStats
{
   public long StreamsAccepted { get; set; }

   public long StreamsOpened { get; set; }
}
