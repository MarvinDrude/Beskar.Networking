using Beskar.Networking.Datagram.Enums;

namespace Beskar.Networking.Datagram.Options;

/// <summary>
/// Configuration for a single logical channel multiplexed over UDP datagrams.
/// </summary>
public sealed class DatagramChannelConfig
{
   /// <summary>
   /// Channel identifier in range <c>0..63</c>.
   /// </summary>
   public byte ChannelId { get; init; }

   /// <summary>
   /// Reliability and ordering semantics for messages sent on this channel.
   /// </summary>
   public DatagramDeliveryMode DeliveryMode { get; init; } = DatagramDeliveryMode.ReliableOrdered;

   /// <summary>
   /// Packing priority when coalescing multiple channels into an MTU-limited datagram.
   /// Higher values are packed first.
   /// </summary>
   public byte Priority { get; init; } = 128;

   /// <summary>
   /// Maximum sliding window size (in messages) for reliable channels. Must be in <c>1..16384</c>.
   /// </summary>
   public int WindowSize { get; init; } = 512;

   /// <summary>
   /// Maximum queued unsent/unacked messages permitted on this channel before applying backpressure.
   /// </summary>
   public int MaxSendQueueDepth { get; init; } = 1024;

   /// <summary>
   /// Whether messages exceeding a single datagram MTU can be split into fragments on this channel.
   /// </summary>
   public bool AllowFragmentation { get; init; } = true;

   /// <summary>
   /// Maximum number of fragments permitted for a single message on this channel.
   /// </summary>
   public int MaxFragmentCount { get; init; } = 128;

   /// <summary>
   /// Maximum concurrent incomplete fragment groups retained on the receive side for this channel.
   /// </summary>
   public int MaxInFlightFragmentGroups { get; init; } = 32;
}
