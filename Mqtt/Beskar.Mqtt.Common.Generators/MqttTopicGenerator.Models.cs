using Beskar.Memory.Collections;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace Beskar.Mqtt.Common.Generators;

public partial class MqttTopicGenerator
{
   public readonly record struct NestingModel(
      string Name,
      string TypeKind);

   public readonly record struct ParameterModel(
      string Name,
      string Type,
      string RefKind);

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

   public readonly record struct GeneratedMethodModel(
      string MethodModifiers,
      string MethodName,
      string ReturnType,
      string NamespaceName,
      SequenceArray<NestingModel> NestingTypes,
      SequenceArray<ParameterModel> Parameters,
      string Pattern,
      bool IsFormatter,
      LocationModel MethodLocation,
      bool IsErrorContainingTypeNotPartial,
      string ErrorContainingTypeName
   );
}
