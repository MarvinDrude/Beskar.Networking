using Beskar.Mqtt.Common.Generators;

namespace Beskar.Jobs.Common.Topics;

/// <summary>
/// Compile-time generated MQTT topic formatting and parsing utilities.
/// </summary>
public static partial class JobTopics
{
   public const string EnqueueCommandTopic = "jobs/commands/enqueue";
   public const string RecurringCommandTopic = "jobs/commands/recurring";
   public const string CancelCommandWildcard = "jobs/commands/cancel/+";
   public const string EventsWildcard = "jobs/events/+/+";
   public const string WorkerHeartbeatWildcard = "jobs/workers/+/heartbeat";
   public const string StatusWildcard = "jobs/status/+";

   [GeneratedMqttTopic("jobs/commands/cancel/{jobId}")]
   public static partial bool TryParseCancelCommand(
      ReadOnlySpan<char> topic,
      out string jobId);

   [GeneratedMqttTopic("jobs/commands/cancel/{jobId}")]
   public static partial bool TryFormatCancelCommand(
      Span<char> destination,
      string jobId,
      out int charsWritten);

   [GeneratedMqttTopic("jobs/dispatch/{queue}/{workerId}")]
   public static partial bool TryParseDispatch(
      ReadOnlySpan<char> topic,
      out string queue,
      out string workerId);

   [GeneratedMqttTopic("jobs/dispatch/{queue}/{workerId}")]
   public static partial bool TryFormatDispatch(
      Span<char> destination,
      string queue,
      string workerId,
      out int charsWritten);

   [GeneratedMqttTopic("jobs/status/{jobId}")]
   public static partial bool TryParseStatus(
      ReadOnlySpan<char> topic,
      out string jobId);

   [GeneratedMqttTopic("jobs/status/{jobId}")]
   public static partial bool TryFormatStatus(
      Span<char> destination,
      string jobId,
      out int charsWritten);

   [GeneratedMqttTopic("jobs/events/{jobId}/{eventType}")]
   public static partial bool TryParseWorkerEvent(
      ReadOnlySpan<char> topic,
      out string jobId,
      out string eventType);

   [GeneratedMqttTopic("jobs/events/{jobId}/{eventType}")]
   public static partial bool TryFormatWorkerEvent(
      Span<char> destination,
      string jobId,
      string eventType,
      out int charsWritten);

   [GeneratedMqttTopic("jobs/workers/{workerId}/heartbeat")]
   public static partial bool TryParseWorkerHeartbeat(
      ReadOnlySpan<char> topic,
      out string workerId);

   [GeneratedMqttTopic("jobs/workers/{workerId}/heartbeat")]
   public static partial bool TryFormatWorkerHeartbeat(
      Span<char> destination,
      string workerId,
      out int charsWritten);
}
