using Beskar.Networking.Datagram.Enums;

namespace Beskar.Networking.Datagram;

/// <summary>
/// Delegate callbacks invoked by <see cref="DatagramHost"/> when running in <see cref="DatagramEventDispatchMode.Async"/>.
/// Register lifecycle and message callbacks before calling <see cref="DatagramHost.Start"/> or <see cref="DatagramHost.ConnectAsync"/>.
/// </summary>
public sealed class DatagramHostEvents
{
   /// <summary>
   /// Optional server-side connection validator invoked after cookie verification and metadata decryption.
   /// Call <see cref="DatagramConnectionRequestContext.Reject"/> to deny the peer.
   /// </summary>
   public Func<DatagramConnectionRequestContext, ValueTask>? ValidateConnection { get; set; }

   /// <summary>
   /// Invoked when a peer completes the handshake and enters <see cref="DatagramPeerState.Connected"/>.
   /// </summary>
   public Func<DatagramPeer, CancellationToken, ValueTask>? PeerConnected { get; set; }

   /// <summary>
   /// Invoked when a peer disconnects or times out.
   /// </summary>
   public Func<DatagramPeer, DatagramDisconnectReason, ReadOnlyMemory<byte>, CancellationToken, ValueTask>? PeerDisconnected { get; set; }

   /// <summary>
   /// Invoked when a channel message is delivered from a peer.
   /// </summary>
   public Func<DatagramPeer, byte, DatagramDeliveryMode, ReadOnlyMemory<byte>, CancellationToken, ValueTask>? MessageReceived { get; set; }

   /// <summary>
   /// Invoked when an authenticated peer migrates to a new remote endpoint.
   /// </summary>
   public Func<DatagramPeer, System.Net.IPEndPoint, System.Net.IPEndPoint, CancellationToken, ValueTask>? PeerMigrated { get; set; }

   /// <summary>
   /// Invoked when a rendezvous server introduces two peers for NAT hole punching.
   /// </summary>
   public Func<Nat.DatagramPunchIntroduction, CancellationToken, ValueTask>? PunchIntroduced { get; set; }
}
