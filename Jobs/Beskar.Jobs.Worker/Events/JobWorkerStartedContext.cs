namespace Beskar.Jobs.Worker.Events;

public sealed record JobWorkerStartedContext(
   string WorkerId,
   string JobId,
   string JobType,
   string Queue,
   int Attempt);
