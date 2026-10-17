namespace Beskar.Jobs.Worker.Events;

public sealed record JobWorkerFailedContext(
   string WorkerId,
   string JobId,
   string JobType,
   string Queue,
   int Attempt,
   string ErrorMessage,
   double DurationMs);
