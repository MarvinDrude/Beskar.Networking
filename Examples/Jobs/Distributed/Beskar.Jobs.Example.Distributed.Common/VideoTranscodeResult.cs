namespace Beskar.Jobs.Example.Distributed.Common;

public sealed record VideoTranscodeResult(
   string OutputUri,
   string ProcessedByWorkerId,
   double DurationSeconds);
