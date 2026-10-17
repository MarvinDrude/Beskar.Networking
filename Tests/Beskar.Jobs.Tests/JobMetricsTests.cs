using System.Diagnostics.Metrics;
using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Common.Telemetry;

namespace Beskar.Jobs.Tests;

[NotInParallel]
public class JobMetricsTests
{
   [Test]
   public async Task JobMetrics_RecordsAllInstrumentsCorrectly()
   {
      long enqueued = 0;
      long dispatched = 0;
      long activeDelta = 0;
      long completed = 0;
      long retries = 0;
      long cronTriggers = 0;
      long workerCrashes = 0;
      long workersOnlineDelta = 0;
      double executionDurationMs = 0;
      double queueLagMs = 0;

      using var listener = new MeterListener();
      listener.InstrumentPublished = (instrument, meterListener) =>
      {
         if (instrument.Meter.Name == JobMetrics.MeterName)
         {
            meterListener.EnableMeasurementEvents(instrument);
         }
      };

      listener.SetMeasurementEventCallback<long>((instrument, measurement, _, _) =>
      {
         switch (instrument.Name)
         {
            case "beskar.jobs.enqueued":
               Interlocked.Add(ref enqueued, measurement);
               break;
            case "beskar.jobs.dispatched":
               Interlocked.Add(ref dispatched, measurement);
               break;
            case "beskar.jobs.active":
               Interlocked.Add(ref activeDelta, measurement);
               break;
            case "beskar.jobs.completed":
               Interlocked.Add(ref completed, measurement);
               break;
            case "beskar.jobs.retries":
               Interlocked.Add(ref retries, measurement);
               break;
            case "beskar.jobs.cron.triggered":
               Interlocked.Add(ref cronTriggers, measurement);
               break;
            case "beskar.jobs.worker.crashes":
               Interlocked.Add(ref workerCrashes, measurement);
               break;
            case "beskar.jobs.workers.online":
               Interlocked.Add(ref workersOnlineDelta, measurement);
               break;
         }
      });

      listener.SetMeasurementEventCallback<double>((instrument, measurement, _, _) =>
      {
         if (instrument.Name == "beskar.jobs.execution.duration")
         {
            Interlocked.Exchange(ref executionDurationMs, measurement);
         }
         else if (instrument.Name == "beskar.jobs.queue.lag")
         {
            Interlocked.Exchange(ref queueLagMs, measurement);
         }
      });

      listener.Start();

      JobMetrics.RecordJobEnqueued("default", "video.transcode");
      JobMetrics.RecordJobDispatched("default", "video.transcode", 12.5);
      JobMetrics.RecordActiveJobsChange(1);
      JobMetrics.RecordExecutionDuration("video.transcode", succeeded: true, 48.2);
      JobMetrics.RecordActiveJobsChange(-1);
      JobMetrics.RecordJobCompleted(JobStatus.Succeeded);
      JobMetrics.RecordJobRetry("video.transcode", 1);
      JobMetrics.RecordCronTriggered("nightly-cleanup");
      JobMetrics.RecordWorkersOnlineChange(1);
      JobMetrics.RecordWorkerCrash("worker-1");

      await Assert.That(Volatile.Read(ref enqueued)).IsGreaterThanOrEqualTo(1);
      await Assert.That(Volatile.Read(ref dispatched)).IsGreaterThanOrEqualTo(1);
      await Assert.That(Volatile.Read(ref completed)).IsGreaterThanOrEqualTo(1);
      await Assert.That(Volatile.Read(ref retries)).IsGreaterThanOrEqualTo(1);
      await Assert.That(Volatile.Read(ref cronTriggers)).IsGreaterThanOrEqualTo(1);
      await Assert.That(Volatile.Read(ref workerCrashes)).IsGreaterThanOrEqualTo(1);
      await Assert.That(Volatile.Read(ref workersOnlineDelta)).IsGreaterThanOrEqualTo(1);
      await Assert.That(Volatile.Read(ref executionDurationMs)).IsEqualTo(48.2);
      await Assert.That(Volatile.Read(ref queueLagMs)).IsEqualTo(12.5);
   }
}
