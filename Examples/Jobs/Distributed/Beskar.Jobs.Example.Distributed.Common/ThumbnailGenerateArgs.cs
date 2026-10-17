namespace Beskar.Jobs.Example.Distributed.Common;

public sealed record ThumbnailGenerateArgs(
   string SourceAsset,
   int FrameCount);
