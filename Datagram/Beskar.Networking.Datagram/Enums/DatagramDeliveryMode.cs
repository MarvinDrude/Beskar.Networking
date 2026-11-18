namespace Beskar.Networking.Datagram.Enums;

/// <summary>
/// Specifies the delivery guarantees and ordering behavior for a datagram channel.
/// </summary>
public enum DatagramDeliveryMode : byte
{
   /// <summary>
   /// Best-effort delivery with no ordering and no retransmission. Lowest overhead.
   /// </summary>
   UnreliableUnordered = 0,

   /// <summary>
   /// Best-effort delivery with sequence tracking; stale out-of-order messages are dropped on receipt.
   /// </summary>
   UnreliableSequenced = 1,

   /// <summary>
   /// Guaranteed delivery with deduplication, delivered immediately upon arrival without ordering stalls.
   /// </summary>
   ReliableUnordered = 2,

   /// <summary>
   /// Guaranteed in-order FIFO delivery per channel using a sliding reorder buffer.
   /// </summary>
   ReliableOrdered = 3,

   /// <summary>
   /// Guaranteed delivery of only the latest state; unacknowledged superseded messages are dropped.
   /// </summary>
   ReliableSequenced = 4
}
