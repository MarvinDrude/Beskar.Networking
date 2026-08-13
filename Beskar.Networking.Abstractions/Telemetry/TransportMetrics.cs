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
   /// Current count of active open connections.
   /// </summary>
   public static readonly UpDownCounter<long> ConnectionsActive = Meter.CreateUpDownCounter<long>(
      "beskar.transport.connections.active",
      "{connection}",
      "Current count of active open network connections.");

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
   /// Current count of active open streams.
   /// </summary>
   public static readonly UpDownCounter<long> StreamsActive = Meter.CreateUpDownCounter<long>(
      "beskar.transport.streams.active",
      "{stream}",
      "Current count of active open network streams.");

   /// <summary>
   /// Total number of streams opened.
   /// </summary>
   public static readonly Counter<long> StreamsOpened = Meter.CreateCounter<long>(
      "beskar.transport.streams.opened",
      "{stream}",
      "Total number of network streams opened.");

   /// <summary>
   /// Total number of streams closed.
   /// </summary>
   public static readonly Counter<long> StreamsClosed = Meter.CreateCounter<long>(
      "beskar.transport.streams.closed",
      "{stream}",
      "Total number of network streams closed.");

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
   /// Total number of memory pool blocks created.
   /// </summary>
   public static readonly Counter<long> MemoryPoolBlocksCreated = Meter.CreateCounter<long>(
      "beskar.transport.memorypool.blocks.created",
      "{block}",
      "Total number of memory pool blocks created.");

   /// <summary>
   /// Total number of memory pool block rent operations.
   /// </summary>
   public static readonly Counter<long> MemoryPoolBlocksRented = Meter.CreateCounter<long>(
      "beskar.transport.memorypool.blocks.rented",
      "{block}",
      "Total number of memory pool block rent operations.");

   /// <summary>
   /// Total number of memory pool block return operations.
   /// </summary>
   public static readonly Counter<long> MemoryPoolBlocksReturned = Meter.CreateCounter<long>(
      "beskar.transport.memorypool.blocks.returned",
      "{block}",
      "Total number of memory pool block return operations.");

   /// <summary>
   /// Current count of active rented memory pool blocks.
   /// </summary>
   public static readonly UpDownCounter<long> MemoryPoolBlocksActive = Meter.CreateUpDownCounter<long>(
      "beskar.transport.memorypool.blocks.active",
      "{block}",
      "Current count of active rented memory pool blocks.");

    /// <summary>
    /// Current count of active listeners.
    /// </summary>
    public static readonly UpDownCounter<long> ListenersActive = Meter.CreateUpDownCounter<long>(
       "beskar.transport.listeners.active",
       "{listener}",
       "Current count of active bound listener sockets.");

    /// <summary>
    /// Total failed connection or accept attempts.
    /// </summary>
    public static readonly Counter<long> ConnectionsFailed = Meter.CreateCounter<long>(
       "beskar.transport.connections.failed",
       "{connection}",
       "Total failed connection or accept attempts.");

    /// <summary>
    /// Duration of TLS handshakes.
    /// </summary>
    public static readonly Histogram<double> TlsHandshakeDuration = Meter.CreateHistogram<double>(
       "beskar.transport.tls.handshake.duration",
       "ms",
       "Duration of TLS handshakes.");

    /// <summary>
    /// Total failed TLS handshakes.
    /// </summary>
    public static readonly Counter<long> TlsHandshakeFailures = Meter.CreateCounter<long>(
       "beskar.transport.tls.handshake.failures",
       "{failure}",
       "Total failed TLS handshakes.");

    /// <summary>
    /// Duration of WebSocket upgrade handshakes.
    /// </summary>
    public static readonly Histogram<double> WsHandshakeDuration = Meter.CreateHistogram<double>(
       "beskar.transport.ws.handshake.duration",
       "ms",
       "Duration of WebSocket upgrade handshakes.");

    /// <summary>
    /// Total failed WebSocket upgrade handshakes.
    /// </summary>
    public static readonly Counter<long> WsHandshakeFailures = Meter.CreateCounter<long>(
       "beskar.transport.ws.handshake.failures",
       "{failure}",
       "Total failed WebSocket upgrade handshakes.");

    /// <summary>
    /// Total number of UDP packets dropped due to full pipeline buffer.
    /// </summary>
    public static readonly Counter<long> UdpPacketsDropped = Meter.CreateCounter<long>(
       "beskar.transport.udp.packets.dropped",
       "{packet}",
       "Total number of UDP packets dropped due to full pipeline buffer.");

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
      var tags = GetTransportTags(kind);
      if (ConnectionsOpened.Enabled)
      {
         ConnectionsOpened.Add(1, tags);
      }
      if (ConnectionsActive.Enabled)
      {
         ConnectionsActive.Add(1, tags);
      }
   }

   public static void RecordConnectionClosed(TransportKind kind)
   {
      var tags = GetTransportTags(kind);
      if (ConnectionsClosed.Enabled)
      {
         ConnectionsClosed.Add(1, tags);
      }
      if (ConnectionsActive.Enabled)
      {
         ConnectionsActive.Add(-1, tags);
      }
   }

   public static void RecordStreamOpened(TransportKind kind)
   {
      var tags = GetTransportTags(kind);
      if (StreamsOpened.Enabled)
      {
         StreamsOpened.Add(1, tags);
      }
      if (StreamsActive.Enabled)
      {
         StreamsActive.Add(1, tags);
      }
   }

   public static void RecordStreamClosed(TransportKind kind)
   {
      var tags = GetTransportTags(kind);
      if (StreamsClosed.Enabled)
      {
         StreamsClosed.Add(1, tags);
      }
      if (StreamsActive.Enabled)
      {
         StreamsActive.Add(-1, tags);
      }
   }

   public static void RecordMemoryPoolBlockCreated()
   {
      if (MemoryPoolBlocksCreated.Enabled)
      {
         MemoryPoolBlocksCreated.Add(1);
      }
   }

   public static void RecordMemoryPoolBlockRented()
   {
      if (MemoryPoolBlocksRented.Enabled)
      {
         MemoryPoolBlocksRented.Add(1);
      }
      if (MemoryPoolBlocksActive.Enabled)
      {
         MemoryPoolBlocksActive.Add(1);
      }
   }

   public static void RecordMemoryPoolBlockReturned()
   {
      if (MemoryPoolBlocksReturned.Enabled)
      {
         MemoryPoolBlocksReturned.Add(1);
      }
      if (MemoryPoolBlocksActive.Enabled)
      {
         MemoryPoolBlocksActive.Add(-1);
      }
   }

   public static void RecordListenerStarted(TransportKind kind)
   {
      if (ListenersActive.Enabled)
      {
         ListenersActive.Add(1, GetTransportTags(kind));
      }
   }

   public static void RecordListenerStopped(TransportKind kind)
   {
      if (ListenersActive.Enabled)
      {
         ListenersActive.Add(-1, GetTransportTags(kind));
      }
   }

   public static void RecordConnectionFailed(TransportKind kind, string failureReason)
   {
      if (ConnectionsFailed.Enabled)
      {
         var tags = GetTransportTags(kind);
         ConnectionsFailed.Add(1,
            tags[0],
            new KeyValuePair<string, object?>("failure", failureReason));
      }
   }

   public static void RecordTlsHandshakeDuration(double milliseconds)
   {
      if (TlsHandshakeDuration.Enabled)
      {
         TlsHandshakeDuration.Record(milliseconds);
      }
   }

   public static void RecordTlsHandshakeFailure(string failureReason)
   {
      if (TlsHandshakeFailures.Enabled)
      {
         TlsHandshakeFailures.Add(1, new KeyValuePair<string, object?>("failure", failureReason));
      }
   }

   public static void RecordWsHandshakeDuration(double milliseconds)
   {
      if (WsHandshakeDuration.Enabled)
      {
         WsHandshakeDuration.Record(milliseconds);
      }
   }

   public static void RecordWsHandshakeFailure(string failureReason)
   {
      if (WsHandshakeFailures.Enabled)
      {
         WsHandshakeFailures.Add(1, new KeyValuePair<string, object?>("failure", failureReason));
      }
   }

   public static void RecordUdpPacketDropped()
   {
      if (UdpPacketsDropped.Enabled)
      {
         UdpPacketsDropped.Add(1);
      }
   }
}
