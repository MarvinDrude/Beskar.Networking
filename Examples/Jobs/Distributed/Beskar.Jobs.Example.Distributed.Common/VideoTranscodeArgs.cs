namespace Beskar.Jobs.Example.Distributed.Common;

public sealed record VideoTranscodeArgs(
   string SourceAsset,
   string TargetCodec,
   int ResolutionHeight);
