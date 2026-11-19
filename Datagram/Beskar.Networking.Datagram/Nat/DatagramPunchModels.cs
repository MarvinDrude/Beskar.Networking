using System.Net;

namespace Beskar.Networking.Datagram.Nat;

/// <summary>
/// Options controlling single-socket UDP NAT hole punching probes.
/// </summary>
public sealed class DatagramPunchOptions
{
   /// <summary>
   /// Interval between outbound hole-punching probe bursts. Defaults to 50 ms.
   /// </summary>
   public TimeSpan ProbeInterval { get; set; } = TimeSpan.FromMilliseconds(50);

   /// <summary>
   /// Total timeout before concluding direct hole punching failed and relay fallback is needed.
   /// Defaults to 3 seconds.
   /// </summary>
   public TimeSpan Timeout { get; set; } = TimeSpan.FromSeconds(3);
}

/// <summary>
/// Result of a NAT hole punching attempt.
/// </summary>
public readonly record struct DatagramPunchResult(
   bool Succeeded,
   IPEndPoint? ResolvedEndPoint,
   TimeSpan RoundTripTime,
   bool NeedsRelayFallback);

/// <summary>
/// Peer introduction payload issued by <see cref="DatagramRendezvousServer"/>.
/// </summary>
public sealed record DatagramPunchIntroduction(
   ReadOnlyMemory<byte> PeerNodeId,
   ulong PunchToken,
   IPEndPoint PeerPublicEndPoint,
   IPEndPoint? PeerLanEndPoint);
