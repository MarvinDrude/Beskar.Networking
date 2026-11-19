using System.Net;

namespace Beskar.Networking.Datagram;

/// <summary>
/// Context passed to connection admission callbacks when a remote peer completes the cookie challenge.
/// </summary>
public sealed class DatagramConnectionRequestContext
{
   public required IPEndPoint RemoteEndPoint { get; init; }
   public required ReadOnlyMemory<byte> ConnectMetadata { get; init; }

   public ulong ChannelConfigHash { get; init; }

   public bool Accepted { get; private set; } = true;
   public byte[]? RejectReason { get; private set; }

   /// <summary>
   /// Optional initial object attached to the newly created <see cref="DatagramPeer.Tag"/>.
   /// </summary>
   public object? PeerTag { get; set; }

   /// <summary>
   /// Rejects the incoming connection with an optional reason payload sent encrypted in <c>ConnectAck</c>.
   /// </summary>
   public void Reject(ReadOnlySpan<byte> reason = default)
   {
      Accepted = false;
      RejectReason = reason.IsEmpty ? null : [.. reason];
   }
}
