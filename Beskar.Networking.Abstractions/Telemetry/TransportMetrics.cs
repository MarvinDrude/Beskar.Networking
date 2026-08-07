using System.Diagnostics.Metrics;
using Beskar.Networking.Abstractions.Enums;

namespace Beskar.Networking.Abstractions.Telemetry;

/// <summary>
/// Contains System.Diagnostics.Metrics telemetry meters and instruments for low-level network transports.
/// </summary>
public static class TransportMetrics
{
   /// <summary>
   /// The meter name for low-level transport metrics.
   /// </summary>
   public const string MeterName = "Beskar.Networking.Transport";

   /// <summary>
   /// Meter instance for Beskar transport metrics.
   /// </summary>
   public static readonly Meter Meter = new(MeterName, "1.0.0");

   /// <summary>
   /// Total number of connection attempts opened.
   /// </summary>
   public static readonly Counter<long> ConnectionsOpened = Meter.CreateCounter<long>(
      "beskar.transport.connections.opened",
      "{connection}",
      "Total number of network connections established.");

   /// <summary>
   /// Total number of connections closed.
   /// </summary>
   public static readonly Counter<long> ConnectionsClosed = Meter.CreateCounter<long>(
      "beskar.transport.connections.closed",
      "{connection}",
      "Total number of network connections closed.");

   /// <summary>
   /// Total payload and frame bytes sent across network pipelines.
   /// </summary>
   public static readonly Counter<long> BytesSent = Meter.CreateCounter<long>(
      "beskar.transport.bytes.sent",
      "By",
      "Total bytes sent over network pipelines.");

   /// <summary>
   /// Total payload and frame bytes received across network pipelines.
   /// </summary>
   public static readonly Counter<long> BytesReceived = Meter.CreateCounter<long>(
      "beskar.transport.bytes.received",
      "By",
      "Total bytes received over network pipelines.");

   /// <summary>
   /// Total packets or frames sent.
   /// </summary>
   public static readonly Counter<long> PacketsSent = Meter.CreateCounter<long>(
      "beskar.transport.packets.sent",
      "{packet}",
      "Total frames/packets sent over transport streams.");

   /// <summary>
   /// Total packets or frames received.
   /// </summary>
   public static readonly Counter<long> PacketsReceived = Meter.CreateCounter<long>(
      "beskar.transport.packets.received",
      "{packet}",
      "Total frames/packets received over transport streams.");

   private static readonly KeyValuePair<string, object?>[][] TransportTags = [
      [new KeyValuePair<string, object?>("transport", "unknown")],
      [new KeyValuePair<string, object?>("transport", "tcp")],
      [new KeyValuePair<string, object?>("transport", "websocket")],
      [new KeyValuePair<string, object?>("transport", "quic")],
      [new KeyValuePair<string, object?>("transport", "udp")],
      [new KeyValuePair<string, object?>("transport", "namedpipe")],
      [new KeyValuePair<string, object?>("transport", "unixdomainsocket")],
      [new KeyValuePair<string, object?>("transport", "memory")]
   ];

   public static ReadOnlySpan<KeyValuePair<string, object?>> GetTransportTags(TransportKind kind)
   {
      var index = (int)kind;
      if (index >= 0 && index < TransportTags.Length)
      {
         return TransportTags[index];
      }
      return TransportTags[0];
   }

   public static void RecordBytesReceived(long bytes, TransportKind kind)
   {
      if (BytesReceived.Enabled)
      {
         BytesReceived.Add(bytes, GetTransportTags(kind));
      }
   }

   public static void RecordBytesSent(long bytes, TransportKind kind)
   {
      if (BytesSent.Enabled)
      {
         BytesSent.Add(bytes, GetTransportTags(kind));
      }
   }

   public static void RecordConnectionOpened(TransportKind kind)
   {
      if (ConnectionsOpened.Enabled)
      {
         ConnectionsOpened.Add(1, GetTransportTags(kind));
      }
   }

   public static void RecordConnectionClosed(TransportKind kind)
   {
      if (ConnectionsClosed.Enabled)
      {
         ConnectionsClosed.Add(1, GetTransportTags(kind));
      }
   }
}
