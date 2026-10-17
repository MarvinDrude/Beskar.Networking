namespace Beskar.Jobs.Common.Interfaces;

/// <summary>
/// Interface implemented automatically by partial job handler classes annotated with
/// <see cref="Attributes.GeneratedJobHandlerAttribute"/>.
/// </summary>
public interface IGeneratedJobDescriptor
{
   /// <summary>
   /// Compile-time job type identifier.
   /// </summary>
   public static abstract string GeneratedJobType { get; }

   /// <summary>
   /// Compile-time default queue name.
   /// </summary>
   public static abstract string GeneratedQueue { get; }
}
