using Beskar.Networking.Datagram.Enums;

namespace Beskar.Networking.Datagram.Telemetry;

/// <summary>
/// Live link quality, bandwidth, and reliability statistics for a <see cref="DatagramPeer"/>.
/// </summary>
public readonly record struct DatagramPeerStats
{
   public double SmoothedRttMs { get; init; }
   public double RttVarianceMs { get; init; }
   public double CurrentRtoMs { get; init; }
   public double PacketLossRate { get; init; }

   public DatagramCongestionState CongestionState { get; init; }
   public float RecommendedSendScale { get; init; }

   public long BytesSent { get; init; }
   public long BytesReceived { get; init; }

   public long DatagramsSent { get; init; }
   public long DatagramsReceived { get; init; }

   public long MessagesRetransmitted { get; init; }
   public long UnackedReliableMessages { get; init; }
   public long EstimatedClockOffsetTicks { get; init; }
}
