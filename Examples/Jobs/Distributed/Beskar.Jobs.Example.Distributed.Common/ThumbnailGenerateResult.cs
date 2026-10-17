namespace Beskar.Jobs.Example.Distributed.Common;

public sealed record ThumbnailGenerateResult(
   string[] SpriteUrls,
   string ProcessedByWorkerId);
