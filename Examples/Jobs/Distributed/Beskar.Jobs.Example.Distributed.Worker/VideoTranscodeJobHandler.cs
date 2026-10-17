using System.Diagnostics;
using Beskar.Jobs.Common.Attributes;
using Beskar.Jobs.Common.Interfaces;
using Beskar.Jobs.Example.Distributed.Common;

namespace Beskar.Jobs.Example.Distributed.Worker;

[GeneratedJobHandler(DistributedClusterConfig.VideoTranscodeJobType, Queue = DistributedClusterConfig.MediaQueue)]
public sealed partial class VideoTranscodeJobHandler : IJobHandlerWithResult<VideoTranscodeArgs, VideoTranscodeResult>
{
   public async ValueTask<VideoTranscodeResult> ExecuteAsync(
      IJobExecutionContext<VideoTranscodeArgs> context,
      CancellationToken ct = default)
   {
      var sw = Stopwatch.StartNew();
      var args = context.Arguments;

      await context.ReportProgressAsync(10, $"[{context.WorkerId}] Probing media container '{args.SourceAsset}'...", ct);
      await Task.Delay(250, ct);

      await context.ReportProgressAsync(45, $"[{context.WorkerId}] Transcoding video stream to {args.TargetCodec} ({args.ResolutionHeight}p)...", ct);
      await Task.Delay(400, ct);

      await context.ReportProgressAsync(85, $"[{context.WorkerId}] Muxing audio and writing HLS segments...", ct);
      await Task.Delay(250, ct);

      sw.Stop();
      var outputUri = $"s3://media-encoded/{args.ResolutionHeight}p/{args.SourceAsset}.{args.TargetCodec}.m3u8";
      return new VideoTranscodeResult(outputUri, context.WorkerId, Math.Round(sw.Elapsed.TotalSeconds, 2));
   }
}
