using Beskar.Jobs.Common.Enums;
using Beskar.Jobs.Common.Models;
using Beskar.Jobs.Common.Storage;

namespace Beskar.Jobs.Tests;

public class InMemoryJobStorageTests
{
   [Test]
   public async Task UpsertAndTransition_EnforcesOptimisticConcurrencyVersion()
   {
      var storage = new InMemoryJobStorage();
      var job = new JobInstanceRecord
      {
         JobId = "job-1",
         JobType = "test.job",
         Queue = "default",
         Status = JobStatus.Enqueued
      };

      await storage.UpsertJobAsync(job);

      var loaded = await storage.GetJobAsync("job-1");
      await Assert.That(loaded).IsNotNull();
      await Assert.That(loaded!.Version).IsEqualTo(1L);

      // Wrong expected version should fail
      var failedTransition = await storage.TryTransitionStateAsync(
         "job-1",
         JobStatus.Enqueued,
         JobStatus.Dispatching,
         expectedVersion: 99);
      await Assert.That(failedTransition).IsNull();

      // Wrong expected status should fail
      var wrongStatusTransition = await storage.TryTransitionStateAsync(
         "job-1",
         JobStatus.Running,
         JobStatus.Succeeded,
         expectedVersion: 1);
      await Assert.That(wrongStatusTransition).IsNull();

      // Correct expected status and version should succeed and increment Version to 2
      var succeededTransition = await storage.TryTransitionStateAsync(
         "job-1",
         JobStatus.Enqueued,
         JobStatus.Dispatching,
         expectedVersion: 1,
         mutate: r => r.AssignedWorkerId = "worker-a");

      await Assert.That(succeededTransition).IsNotNull();
      await Assert.That(succeededTransition!.Status).IsEqualTo(JobStatus.Dispatching);
      await Assert.That(succeededTransition.Version).IsEqualTo(2L);
      await Assert.That(succeededTransition.AssignedWorkerId).IsEqualTo("worker-a");
   }

   [Test]
   public async Task GetJobAsync_ReturnsDeepClone_ExternalMutationsDoNotCorruptStorage()
   {
      var storage = new InMemoryJobStorage();
      var originalPayload = new byte[] { 1, 2, 3 };
      var metadata = new Dictionary<string, string> { ["env"] = "prod" };

      await storage.UpsertJobAsync(new JobInstanceRecord
      {
         JobId = "clone-test",
         JobType = "test",
         Payload = originalPayload,
         Metadata = metadata
      });

      // Mutate original arrays/dictionaries
      originalPayload[0] = 99;
      metadata["env"] = "hacked";

      var fetched = await storage.GetJobAsync("clone-test");
      await Assert.That(fetched).IsNotNull();
      await Assert.That(fetched!.Payload[0]).IsEqualTo((byte)1);
      await Assert.That(fetched.Metadata!["env"]).IsEqualTo("prod");

      // Mutate fetched record and verify storage remains untouched
      fetched.Status = JobStatus.Cancelled;
      fetched.Payload[1] = 88;

      var fetchedAgain = await storage.GetJobAsync("clone-test");
      await Assert.That(fetchedAgain!.Status).IsEqualTo(JobStatus.Enqueued);
      await Assert.That(fetchedAgain.Payload[1]).IsEqualTo((byte)2);
   }

   [Test]
   public async Task TryUpdateJobAsync_RespectsAllowIfTerminalFlag()
   {
      var storage = new InMemoryJobStorage();
      await storage.UpsertJobAsync(new JobInstanceRecord
      {
         JobId = "term-1",
         JobType = "test",
         Status = JobStatus.Succeeded
      });

      var blockedUpdate = await storage.TryUpdateJobAsync(
         "term-1",
         r => r.ProgressMessage = "ShouldNotApply",
         allowIfTerminal: false);
      await Assert.That(blockedUpdate).IsNull();

      var allowedUpdate = await storage.TryUpdateJobAsync(
         "term-1",
         r => r.ProgressMessage = "AppliedOnTerminal",
         allowIfTerminal: true);
      await Assert.That(allowedUpdate).IsNotNull();
      await Assert.That(allowedUpdate!.ProgressMessage).IsEqualTo("AppliedOnTerminal");
   }

   [Test]
   public async Task ClaimDueJobsAsync_ClaimsEligibleAndSkipsFutureOrUnresolvedContinuations()
   {
      var storage = new InMemoryJobStorage();
      var now = DateTimeOffset.UtcNow;

      // 1. Due immediate job
      await storage.UpsertJobAsync(new JobInstanceRecord
      {
         JobId = "due-1",
         JobType = "work",
         Status = JobStatus.Enqueued,
         ScheduledAtUtc = now.AddSeconds(-5)
      });

      // 2. Future delayed job
      await storage.UpsertJobAsync(new JobInstanceRecord
      {
         JobId = "future-1",
         JobType = "work",
         Status = JobStatus.Scheduled,
         ScheduledAtUtc = now.AddMinutes(10)
      });

      // 3. Child continuation waiting on parent
      await storage.UpsertJobAsync(new JobInstanceRecord
      {
         JobId = "child-1",
         JobType = "work",
         Status = JobStatus.Scheduled,
         ParentJobId = "due-1",
         ScheduledAtUtc = now.AddSeconds(-1)
      });

      var claimed = await storage.ClaimDueJobsAsync(now, maxBatchSize: 10, lockDuration: TimeSpan.FromSeconds(30));
      await Assert.That(claimed.Count).IsEqualTo(1);
      await Assert.That(claimed[0].JobId).IsEqualTo("due-1");
      await Assert.That(claimed[0].Status).IsEqualTo(JobStatus.Dispatching);

      // Re-claiming immediately should return 0 because 'due-1' is locked
      var secondClaim = await storage.ClaimDueJobsAsync(now, maxBatchSize: 10, lockDuration: TimeSpan.FromSeconds(30));
      await Assert.That(secondClaim.Count).IsEqualTo(0);

      // After lock expiry, 'due-1' can be reclaimed if still Dispatching
      var expiredClaim = await storage.ClaimDueJobsAsync(now.AddSeconds(35), maxBatchSize: 10, lockDuration: TimeSpan.FromSeconds(30));
      await Assert.That(expiredClaim.Count).IsEqualTo(1);
      await Assert.That(expiredClaim[0].JobId).IsEqualTo("due-1");
   }

   [Test]
   public async Task GetActiveJobsByWorkerAndContinuations_FilterAccurately()
   {
      var storage = new InMemoryJobStorage();

      await storage.UpsertJobAsync(new JobInstanceRecord
      {
         JobId = "w1-running",
         JobType = "work",
         Status = JobStatus.Running,
         AssignedWorkerId = "worker-1"
      });

      await storage.UpsertJobAsync(new JobInstanceRecord
      {
         JobId = "w1-done",
         JobType = "work",
         Status = JobStatus.Succeeded,
         AssignedWorkerId = "worker-1"
      });

      await storage.UpsertJobAsync(new JobInstanceRecord
      {
         JobId = "w2-running",
         JobType = "work",
         Status = JobStatus.Running,
         AssignedWorkerId = "worker-2"
      });

      await storage.UpsertJobAsync(new JobInstanceRecord
      {
         JobId = "cont-child",
         JobType = "work",
         Status = JobStatus.Scheduled,
         ParentJobId = "w1-running"
      });

      var worker1Active = await storage.GetActiveJobsByWorkerAsync("worker-1");
      await Assert.That(worker1Active.Count).IsEqualTo(1);
      await Assert.That(worker1Active[0].JobId).IsEqualTo("w1-running");

      var continuations = await storage.GetContinuationJobsAsync("w1-running");
      await Assert.That(continuations.Count).IsEqualTo(1);
      await Assert.That(continuations[0].JobId).IsEqualTo("cont-child");
   }

   [Test]
   public async Task RecurringAndPruning_RespectsPausedFlagAndPrunesExpired()
   {
      var storage = new InMemoryJobStorage();
      var now = DateTimeOffset.UtcNow;

      await storage.UpsertRecurringJobAsync(new RecurringJobRecord
      {
         RecurringJobId = "rec-active",
         CronExpression = "@hourly",
         JobType = "cleanup",
         IsPaused = false,
         NextRunAtUtc = now.AddMinutes(-1)
      });

      await storage.UpsertRecurringJobAsync(new RecurringJobRecord
      {
         RecurringJobId = "rec-paused",
         CronExpression = "@hourly",
         JobType = "cleanup",
         IsPaused = true,
         NextRunAtUtc = now.AddMinutes(-1)
      });

      var due = await storage.GetDueRecurringJobsAsync(now);
      await Assert.That(due.Count).IsEqualTo(1);
      await Assert.That(due[0].RecurringJobId).IsEqualTo("rec-active");

      var fetchedPaused = await storage.GetRecurringJobAsync("rec-paused");
      await Assert.That(fetchedPaused).IsNotNull();
      await Assert.That(fetchedPaused!.IsPaused).IsTrue();

      await storage.RemoveRecurringJobAsync("rec-active");
      await storage.RemoveRecurringJobAsync("rec-paused");
      await Assert.That(storage.RecurringJobCount).IsEqualTo(0);

      // Completed old job pruning
      await storage.UpsertJobAsync(new JobInstanceRecord
      {
         JobId = "old-done",
         JobType = "cleanup",
         Status = JobStatus.Succeeded,
         CompletedAtUtc = now.AddHours(-2)
      });

      var prunedCount = await storage.PruneCompletedJobsAsync(now.AddHours(-1));
      await Assert.That(prunedCount).IsEqualTo(1);
      await Assert.That(storage.JobCount).IsEqualTo(0);
   }

   [Test]
   public async Task ConcurrentClaimDueJobs_GuaranteesEachJobClaimedAtMostOnce()
   {
      var storage = new InMemoryJobStorage();
      var now = DateTimeOffset.UtcNow;
      const int TotalJobs = 100;

      for (var i = 0; i < TotalJobs; i++)
      {
         await storage.UpsertJobAsync(new JobInstanceRecord
         {
            JobId = $"conc-job-{i:D3}",
            JobType = "work",
            Status = JobStatus.Enqueued,
            ScheduledAtUtc = now.AddSeconds(-10)
         });
      }

      var claimTasks = Enumerable.Range(0, 10).Select(_ =>
         Task.Run(async () => await storage.ClaimDueJobsAsync(now, maxBatchSize: 15, lockDuration: TimeSpan.FromMinutes(1))));

      var batches = await Task.WhenAll(claimTasks);
      var allClaimedIds = batches.SelectMany(b => b.Select(j => j.JobId)).ToList();

      await Assert.That(allClaimedIds.Count).IsEqualTo(TotalJobs);
      await Assert.That(allClaimedIds.Distinct(StringComparer.Ordinal).Count()).IsEqualTo(TotalJobs);
   }
}
