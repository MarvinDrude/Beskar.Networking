namespace Beskar.Networking.Datagram.Enums;

/// <summary>
/// Lifecycle state of a <see cref="DatagramPeer"/>.
/// </summary>
public enum DatagramPeerState : byte
{
   /// <summary>
   /// Initial handshake in progress.
   /// </summary>
   Connecting = 0,

   /// <summary>
   /// Handshake verified and session active.
   /// </summary>
   Connected = 1,

   /// <summary>
   /// Graceful disconnect sequence in progress.
   /// </summary>
   Disconnecting = 2,

   /// <summary>
   /// Peer disconnected and resources released.
   /// </summary>
   Disconnected = 3
}
