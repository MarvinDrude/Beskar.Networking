namespace Beskar.Jobs.Worker.Events;

public sealed record JobWorkerCancelledContext(
   string WorkerId,
   string JobId,
   string JobType,
   string Queue,
   int Attempt);
