namespace Beskar.Jobs.Common.Attributes;

/// <summary>
/// Marks a partial job handler class for compile-time job metadata generation.
/// </summary>
[AttributeUsage(AttributeTargets.Class, Inherited = false, AllowMultiple = false)]
public sealed class GeneratedJobHandlerAttribute(string jobType) : Attribute
{
   /// <summary>
   /// Unique job type identifier (e.g. "video.transcode").
   /// </summary>
   public string JobType { get; } = jobType;

   /// <summary>
   /// Default queue name for this job handler. Defaults to "default".
   /// </summary>
   public string Queue { get; set; } = "default";
}
