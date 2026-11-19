using System.Diagnostics.Metrics;

namespace Beskar.Networking.Datagram.Telemetry;

/// <summary>
/// System.Diagnostics.Metrics telemetry instruments for <see cref="Beskar.Networking.Datagram"/>.
/// </summary>
public static class DatagramMetrics
{
   public const string MeterName = "Beskar.Networking.Datagram";
   public static readonly Meter Meter = new(MeterName, "1.0.0");

   public static readonly UpDownCounter<long> PeersActive = Meter.CreateUpDownCounter<long>(
      "beskar.datagram.peers.active",
      "{peer}",
      "Current number of active connected peers.");

   public static readonly Counter<long> PeersConnected = Meter.CreateCounter<long>(
      "beskar.datagram.peers.connected",
      "{peer}",
      "Total peers connected.");

   public static readonly Counter<long> PeersDisconnected = Meter.CreateCounter<long>(
      "beskar.datagram.peers.disconnected",
      "{peer}",
      "Total peers disconnected.");

   public static readonly Counter<long> DatagramsSent = Meter.CreateCounter<long>(
      "beskar.datagram.packets.sent",
      "{packet}",
      "Total physical UDP datagrams sent.");

   public static readonly Counter<long> DatagramsReceived = Meter.CreateCounter<long>(
      "beskar.datagram.packets.received",
      "{packet}",
      "Total valid UDP datagrams received.");

   public static readonly Counter<long> PacketsRetransmitted = Meter.CreateCounter<long>(
      "beskar.datagram.messages.retransmitted",
      "{message}",
      "Total reliable channel messages/fragments retransmitted.");

   public static readonly Counter<long> SecurityRejections = Meter.CreateCounter<long>(
      "beskar.datagram.security.rejected",
      "{packet}",
      "Total packets rejected due to CRC, AEAD auth, replay, or invalid cookie failures.");

   public static readonly Counter<long> BytesSent = Meter.CreateCounter<long>(
      "beskar.datagram.bytes.sent",
      "By",
      "Total UDP datagram bytes sent.");

   public static readonly Counter<long> BytesReceived = Meter.CreateCounter<long>(
      "beskar.datagram.bytes.received",
      "By",
      "Total UDP datagram bytes received.");

   public static readonly Histogram<double> RoundTripTimeMs = Meter.CreateHistogram<double>(
      "beskar.datagram.rtt",
      "ms",
      "Measured round-trip time in milliseconds.");
}
