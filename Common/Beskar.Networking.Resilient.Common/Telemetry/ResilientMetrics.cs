using System.Diagnostics.Metrics;

namespace Beskar.Networking.Resilient.Common.Telemetry;

/// <summary>
/// Contains System.Diagnostics.Metrics telemetry meters and instruments for resilient network client and server wrappers.
/// </summary>
public static class ResilientMetrics
{
   /// <summary>
   /// The meter name for resilient network metrics.
   /// </summary>
   public const string MeterName = "Beskar.Networking.Resilient";

   /// <summary>
   /// Meter instance for Beskar resilient metrics.
   /// </summary>
   public static readonly Meter Meter = new(MeterName, "1.0.0");

   /// <summary>
   /// Current count of active resilient sessions.
   /// </summary>
   public static readonly UpDownCounter<long> SessionsActive = Meter.CreateUpDownCounter<long>(
      "beskar.resilient.sessions.active",
      "{session}",
      "Current count of active resilient sessions.");

   /// <summary>
   /// Total number of auto-reconnect attempts triggered.
   /// </summary>
   public static readonly Counter<long> ReconnectAttempts = Meter.CreateCounter<long>(
      "beskar.resilient.reconnect.attempts",
      "{attempt}",
      "Total reconnection attempts made by resilient clients.");

   /// <summary>
   /// Duration of reconnection attempts in milliseconds.
   /// </summary>
   public static readonly Histogram<double> ReconnectDuration = Meter.CreateHistogram<double>(
      "beskar.resilient.reconnect.duration",
      "ms",
      "Time taken for reconnection attempt until connected or failed.");

   /// <summary>
   /// Round-trip time (RTT) for keep-alive ping/pong exchanges in milliseconds.
   /// </summary>
   public static readonly Histogram<double> PingRtt = Meter.CreateHistogram<double>(
      "beskar.resilient.ping.rtt",
      "ms",
      "Round-trip latency for protocol ping/pong keep-alive frames.");

   /// <summary>
   /// Count of missed keep-alive ping/pong timeouts resulting in disconnect.
   /// </summary>
   public static readonly Counter<long> PingTimeouts = Meter.CreateCounter<long>(
      "beskar.resilient.ping.timeouts",
      "{timeout}",
      "Total keep-alive ping/pong timeouts.");

   /// <summary>
   /// Total challenge-response authentication attempts.
   /// </summary>
   public static readonly Counter<long> AuthAttempts = Meter.CreateCounter<long>(
      "beskar.resilient.auth.attempts",
      "{attempt}",
      "Total challenge authentication attempts.");

   /// <summary>
   /// Current size of offline message queue buffering during disconnects.
   /// </summary>
   public static readonly UpDownCounter<long> OfflineQueueSize = Meter.CreateUpDownCounter<long>(
      "beskar.resilient.offline_queue.size",
      "{frame}",
      "Current count of buffered frames in offline queue.");

   /// <summary>
   /// Total frames dropped from offline buffer during disconnection.
   /// </summary>
   public static readonly Counter<long> OfflineQueueDropped = Meter.CreateCounter<long>(
      "beskar.resilient.offline_queue.dropped",
      "{frame}",
      "Total frames dropped from offline message queue.");

   private static readonly KeyValuePair<string, object?>[] TagSuccess = [new KeyValuePair<string, object?>("status", "success")];
   private static readonly KeyValuePair<string, object?>[] TagFailed = [new KeyValuePair<string, object?>("status", "failed")];
   private static readonly KeyValuePair<string, object?>[] TagRoleClient = [new KeyValuePair<string, object?>("role", "client")];
   private static readonly KeyValuePair<string, object?>[] TagRoleServer = [new KeyValuePair<string, object?>("role", "server")];

   public static void RecordSessionStateChange(int delta, bool isClient)
   {
      if (SessionsActive.Enabled)
      {
         SessionsActive.Add(delta, isClient ? TagRoleClient : TagRoleServer);
      }
   }

   public static void RecordReconnectAttempt(bool success, double durationMs)
   {
      var tags = success ? TagSuccess : TagFailed;
      if (ReconnectAttempts.Enabled)
      {
         ReconnectAttempts.Add(1, tags);
      }
      if (ReconnectDuration.Enabled)
      {
         ReconnectDuration.Record(durationMs, tags);
      }
   }

   public static void RecordPingRtt(double rttMs, bool isClient)
   {
      if (PingRtt.Enabled)
      {
         PingRtt.Record(rttMs, isClient ? TagRoleClient : TagRoleServer);
      }
   }

   public static void RecordPingTimeout(bool isClient)
   {
      if (PingTimeouts.Enabled)
      {
         PingTimeouts.Add(1, isClient ? TagRoleClient : TagRoleServer);
      }
   }

   public static void RecordAuthAttempt(bool success)
   {
      if (AuthAttempts.Enabled)
      {
         AuthAttempts.Add(1, success ? TagSuccess : TagFailed);
      }
   }

   public static void RecordOfflineQueueSizeChange(int delta)
   {
      if (OfflineQueueSize.Enabled && delta != 0)
      {
         OfflineQueueSize.Add(delta);
      }
   }

   public static void RecordOfflineQueueDropped(int count = 1)
   {
      if (OfflineQueueDropped.Enabled && count > 0)
      {
         OfflineQueueDropped.Add(count);
      }
   }
}
