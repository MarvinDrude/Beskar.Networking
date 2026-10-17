namespace Beskar.Jobs.Worker.Events;

public sealed record JobWorkerCompletedContext(
   string WorkerId,
   string JobId,
   string JobType,
   string Queue,
   int Attempt,
   double DurationMs);
