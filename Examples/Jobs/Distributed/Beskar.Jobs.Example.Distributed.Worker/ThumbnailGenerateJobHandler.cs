using Beskar.Jobs.Common.Attributes;
using Beskar.Jobs.Common.Interfaces;
using Beskar.Jobs.Example.Distributed.Common;

namespace Beskar.Jobs.Example.Distributed.Worker;

[GeneratedJobHandler(DistributedClusterConfig.ThumbnailGenerateJobType, Queue = DistributedClusterConfig.MediaQueue)]
public sealed partial class ThumbnailGenerateJobHandler : IJobHandlerWithResult<ThumbnailGenerateArgs, ThumbnailGenerateResult>
{
   public async ValueTask<ThumbnailGenerateResult> ExecuteAsync(
      IJobExecutionContext<ThumbnailGenerateArgs> context,
      CancellationToken ct = default)
   {
      var args = context.Arguments;
      var urls = new string[args.FrameCount];

      for (var i = 0; i < args.FrameCount; i++)
      {
         ct.ThrowIfCancellationRequested();
         var pct = (int)((i + 1) * 100.0 / args.FrameCount);
         await context.ReportProgressAsync(pct, $"[{context.WorkerId}] Extracted keyframe #{i + 1}/{args.FrameCount} for '{args.SourceAsset}'", ct);
         await Task.Delay(120, ct);
         urls[i] = $"s3://media-thumbs/{args.SourceAsset}/thumb_{i + 1:D2}.jpg";
      }

      return new ThumbnailGenerateResult(urls, context.WorkerId);
   }
}
