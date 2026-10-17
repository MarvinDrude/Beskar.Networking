using Beskar.Memory.Collections;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace Beskar.Jobs.Generator;

public partial class BeskarJobGenerator
{
   public readonly record struct NestingModel(
      string Name,
      string TypeKind);

   public readonly record struct LocationModel(
      string FilePath,
      int SourceSpanStart,
      int SourceSpanLength)
   {
      public Location ToLocation()
      {
         return Location.Create(
            FilePath,
            new TextSpan(SourceSpanStart, SourceSpanLength),
            new LinePositionSpan(default, default));
      }
   }

   public readonly record struct GeneratedJobHandlerModel(
      string TypeName,
      string TypeKind,
      string Accessibility,
      string NamespaceName,
      SequenceArray<NestingModel> NestingTypes,
      string JobType,
      string Queue,
      LocationModel TypeLocation,
      bool IsErrorTypeNotPartial,
      string ErrorNonPartialTypeName,
      bool IsErrorInvalidJobType,
      bool IsErrorInvalidQueue,
      bool IsErrorMissingInterface
   );
}
