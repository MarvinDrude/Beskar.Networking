namespace Beskar.Networking.Abstractions.Models;

/// <summary>
/// Represents lightweight network statistics for bytes received and sent.
/// </summary>
public struct NetworkStats
{
   /// <summary>
   /// The number of bytes received.
   /// </summary>
   public long BytesReceived { get; set; }

   /// <summary>
   /// The number of bytes sent.
   /// </summary>
   public long BytesSent { get; set; }

   /// <summary>
   /// The timestamp when the last byte was received.
   /// </summary>
   public DateTimeOffset? LastReceivedTimestamp { get; set; }

   /// <summary>
   /// The timestamp when the last byte was sent.
   /// </summary>
   public DateTimeOffset? LastSentTimestamp { get; set; }
}
