namespace Beskar.Networking.Datagram.Enums;

/// <summary>
/// Identifies the kind of event emitted by <see cref="DatagramHost"/>.
/// </summary>
public enum DatagramEventType : byte
{
   /// <summary>
   /// A new peer completed handshake and is ready for data exchange.
   /// </summary>
   PeerConnected = 1,

   /// <summary>
   /// A connected peer disconnected gracefully or timed out.
   /// </summary>
   PeerDisconnected = 2,

   /// <summary>
   /// A channel message arrived from a connected peer.
   /// </summary>
   MessageReceived = 3,

   /// <summary>
   /// An authenticated peer seamlessly migrated to a new remote IP/port (e.g., NAT rebind).
   /// </summary>
   PeerMigrated = 4
}
