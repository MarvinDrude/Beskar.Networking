using System.Collections.Frozen;
using Beskar.Jobs.Common.Interfaces;

namespace Beskar.Jobs.Worker.Internal;

internal sealed class JobExecutionContext<TArgs>(
   string jobId,
   string jobType,
   string queue,
   int attempt,
   int maxAttempts,
   string workerId,
   TArgs arguments,
   IReadOnlyDictionary<string, string>? metadata,
   Func<int, string?, CancellationToken, ValueTask> reportProgressCallback)
   : IJobExecutionContext<TArgs>
{
   // ReSharper disable once StaticMemberInGenericType
   private static readonly IReadOnlyDictionary<string, string> EmptyMetadata =
      FrozenDictionary<string, string>.Empty;

   private readonly Func<int, string?, CancellationToken, ValueTask> _reportProgressCallback = reportProgressCallback;

   public string JobId { get; } = jobId;
   public string JobType { get; } = jobType;
   public string Queue { get; } = queue;

   public int Attempt { get; } = attempt;
   public int MaxAttempts { get; } = maxAttempts;

   public string WorkerId { get; } = workerId;
   public TArgs Arguments { get; } = arguments;

   public IReadOnlyDictionary<string, string> Metadata { get; } = metadata ?? EmptyMetadata;

   public ValueTask ReportProgressAsync(int progressPercent, string? message = null, CancellationToken ct = default)
   {
      var clamped = Math.Clamp(progressPercent, 0, 100);
      return _reportProgressCallback(clamped, message, ct);
   }
}
