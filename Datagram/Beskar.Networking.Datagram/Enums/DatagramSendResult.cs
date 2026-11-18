namespace Beskar.Networking.Datagram.Enums;

/// <summary>
/// Outcome of attempting to enqueue an outgoing channel message.
/// </summary>
public enum DatagramSendResult : byte
{
   /// <summary>
   /// Message was queued for transmission.
   /// </summary>
   Queued = 0,

   /// <summary>
   /// Peer is not currently connected.
   /// </summary>
   PeerNotConnected = 1,

   /// <summary>
   /// Channel ID is not registered in <see cref="DatagramOptions"/>.
   /// </summary>
   InvalidChannel = 2,

   /// <summary>
   /// Payload exceeds the maximum single-message or max-fragmented size for the channel.
   /// </summary>
   PayloadTooLarge = 3,

   /// <summary>
   /// Channel send queue or reliability window is full (apply backpressure or wait for ACKs).
   /// </summary>
   QueueFull = 4
}

/// <summary>
/// Indicates link health inferred from rolling packet loss and RTT inflation.
/// </summary>
public enum DatagramCongestionState : byte
{
   Healthy = 0,
   Congested = 1,
   Critical = 2
}

/// <summary>
/// Reason a peer session ended.
/// </summary>
public enum DatagramDisconnectReason : byte
{
   GracefulLocal = 0,
   GracefulRemote = 1,
   Timeout = 2,
   HandshakeRejected = 3,
   MaxRetriesExceeded = 4,
   HostStopped = 5
}
