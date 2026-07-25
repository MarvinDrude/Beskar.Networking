using Beskar.Memory.Collections;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.Text;

namespace Beskar.Networking.Protocol.Generators;

public partial class FramingProtocolGenerator
{
   public enum ProtocolAttributeKind
   {
      MagicBytes,
      VersionField,
      ProtocolField,
      FlagsField,
      VarNumberField,
      ByteSequenceField
   }

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

   public readonly record struct ProtocolPropertyModel(
      string PropertyName,
      string PropertyType,
      ProtocolAttributeKind AttributeKind,
      int Order,
      bool IsPartial,
      SequenceArray<byte> MagicBytes,
      string? LengthPropertyName,
      bool SafeCopyData,
      LocationModel Location);

   public readonly record struct GeneratedTypeModel(
      string TypeName,
      string TypeKind,
      string NamespaceName,
      SequenceArray<NestingModel> NestingTypes,
      SequenceArray<ProtocolPropertyModel> Properties,
      LocationModel TypeLocation,
      bool IsErrorTypeNotPartial,
      bool IsValueType
   );
}
