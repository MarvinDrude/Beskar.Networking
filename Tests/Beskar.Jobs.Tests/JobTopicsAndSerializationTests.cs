using System.Text;
using Beskar.Jobs.Common.Attributes;
using Beskar.Jobs.Common.Interfaces;
using Beskar.Jobs.Common.Models;
using Beskar.Jobs.Common.Serialization;
using Beskar.Jobs.Common.Topics;
using Beskar.Networking.Abstractions.Backoffs;

namespace Beskar.Jobs.Tests;

public partial class JobTopicsAndSerializationTests
{
   [GeneratedJobHandler("nested.analytics.job", Queue = "analytics")]
   public sealed partial class NestedAnalyticsJobHandler : IJobHandlerWithResult<MultiplyJobArgs, MultiplyJobResult>
   {
      public ValueTask<MultiplyJobResult> ExecuteAsync(IJobExecutionContext<MultiplyJobArgs> context, CancellationToken ct = default)
         => ValueTask.FromResult(new MultiplyJobResult(context.Arguments.A + context.Arguments.B, context.WorkerId));
   }

   [Test]
   public async Task BeskarJobGenerator_GeneratesDescriptorForTopLevelAndNestedHandlers()
   {
      await Assert.That(GeneratedSumJobHandler.JobType).IsEqualTo("math.generated.sum");
      await Assert.That(GeneratedSumJobHandler.DefaultQueue).IsEqualTo("math");
      await Assert.That(GeneratedSumJobHandler.GeneratedJobType).IsEqualTo("math.generated.sum");
      await Assert.That(GeneratedSumJobHandler.GeneratedQueue).IsEqualTo("math");

      await Assert.That(NestedAnalyticsJobHandler.JobType).IsEqualTo("nested.analytics.job");
      await Assert.That(NestedAnalyticsJobHandler.DefaultQueue).IsEqualTo("analytics");
      await Assert.That(NestedAnalyticsJobHandler.GeneratedJobType).IsEqualTo("nested.analytics.job");
      await Assert.That(NestedAnalyticsJobHandler.GeneratedQueue).IsEqualTo("analytics");
   }

   [Test]
   public async Task JobTopics_FormatAndParse_RoundTripsAllCompiledTopics()
   {
      // 1. Cancel Command Topic
      var cancelTopic = JobTopics.FormatCancelCommand("job-123");
      await Assert.That(cancelTopic).IsEqualTo("jobs/commands/cancel/job-123");
      await Assert.That(JobTopics.TryParseCancelCommand(cancelTopic, out var parsedCancelJobId)).IsTrue();
      await Assert.That(parsedCancelJobId).IsEqualTo("job-123");
      await Assert.That(JobTopics.TryParseCancelCommand("jobs/commands/other/job-123", out _)).IsFalse();

      var cancelBytes = JobTopics.FormatCancelCommandToBytes("job-123");
      await Assert.That(Encoding.UTF8.GetString(cancelBytes)).IsEqualTo(cancelTopic);

      Span<char> smallBuffer = stackalloc char[5];
      await Assert.That(JobTopics.TryFormatCancelCommand(smallBuffer, "job-123", out _)).IsFalse();

      // 2. Dispatch Topic
      var dispatchTopic = JobTopics.FormatDispatch("media", "worker-01");
      await Assert.That(dispatchTopic).IsEqualTo("jobs/dispatch/media/worker-01");
      await Assert.That(JobTopics.TryParseDispatch(dispatchTopic, out var parsedQueue, out var parsedWorker)).IsTrue();
      await Assert.That(parsedQueue).IsEqualTo("media");
      await Assert.That(parsedWorker).IsEqualTo("worker-01");
      await Assert.That(JobTopics.TryParseDispatch("jobs/dispatch/media", out _, out _)).IsFalse();

      // 3. Status Topic
      var statusTopic = JobTopics.FormatStatus("job-999");
      await Assert.That(statusTopic).IsEqualTo("jobs/status/job-999");
      await Assert.That(JobTopics.TryParseStatus(statusTopic, out var statusJobId)).IsTrue();
      await Assert.That(statusJobId).IsEqualTo("job-999");
      await Assert.That(JobTopics.TryParseStatus("jobs/other/job-999", out _)).IsFalse();

      // 4. Worker Event Topic
      var eventTopic = JobTopics.FormatWorkerEvent("job-999", "completed");
      await Assert.That(eventTopic).IsEqualTo("jobs/events/job-999/completed");
      await Assert.That(JobTopics.TryParseWorkerEvent(eventTopic, out var evtJobId, out var evtType)).IsTrue();
      await Assert.That(evtJobId).IsEqualTo("job-999");
      await Assert.That(evtType).IsEqualTo("completed");

      // 5. Worker Heartbeat Topic
      var hbTopic = JobTopics.FormatWorkerHeartbeat("worker-alpha");
      await Assert.That(hbTopic).IsEqualTo("jobs/workers/worker-alpha/heartbeat");
      await Assert.That(JobTopics.TryParseWorkerHeartbeat(hbTopic, out var hbWorkerId)).IsTrue();
      await Assert.That(hbWorkerId).IsEqualTo("worker-alpha");
   }

   [Test]
   public async Task JsonJobPayloadSerializer_HandlesRecordsRawBytesAndEmptyPayloads()
   {
      var serializer = JsonJobPayloadSerializer.Default;

      // 1. Typed record round-trip
      var original = new MultiplyJobArgs(12, 34);
      var bytes = serializer.Serialize(original);
      var decoded = serializer.Deserialize<MultiplyJobArgs>(bytes);
      await Assert.That(decoded).IsEqualTo(original);

      // 2. Raw byte[] passthrough without JSON base64 wrapping overhead
      var raw = new byte[] { 10, 20, 30, 40 };
      var rawSerialized = serializer.Serialize(raw);
      await Assert.That(rawSerialized.SequenceEqual(raw)).IsTrue();
      var rawDecoded = serializer.Deserialize<byte[]>(rawSerialized);
      await Assert.That(rawDecoded!.SequenceEqual(raw)).IsTrue();

      // 3. Empty span returns default
      var emptyDecoded = serializer.Deserialize<MultiplyJobArgs>(ReadOnlySpan<byte>.Empty);
      await Assert.That(emptyDecoded).IsNull();
   }

   [Test]
   public async Task RetryPolicy_ClampsDelayToMaxDelay()
   {
      var policy = new RetryPolicy
      {
         MaxAttempts = 5,
         Backoff = new ExponentialBackoffPolicy(TimeSpan.FromSeconds(2)),
         MaxDelay = TimeSpan.FromSeconds(5)
      };

      await Assert.That(policy.GetRetryDelay(1)).IsEqualTo(TimeSpan.FromSeconds(2));
      await Assert.That(policy.GetRetryDelay(2)).IsEqualTo(TimeSpan.FromSeconds(4));
      // Attempt 3 would be 8s, but MaxDelay clamps it to 5s
      await Assert.That(policy.GetRetryDelay(3)).IsEqualTo(TimeSpan.FromSeconds(5));
   }
}
