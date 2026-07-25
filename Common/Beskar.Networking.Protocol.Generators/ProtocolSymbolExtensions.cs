using Microsoft.CodeAnalysis;

namespace Beskar.Networking.Protocol.Generators;

internal static class ProtocolSymbolExtensions
{
   extension(ISymbol? symbol)
   {
      public bool IsGenerateFramingProtocolAttribute()
      {
         if (symbol is null) return false;
         var targetType = symbol is IMethodSymbol methodSym ? methodSym.ContainingType : symbol as INamedTypeSymbol;

         return targetType?.Name is "GenerateFramingProtocolAttribute" or "GenerateFramingProtocol";
      }

      public bool IsProtocolAttribute(out FramingProtocolGenerator.ProtocolAttributeKind kind)
      {
         kind = default;
         if (symbol is null) return false;

         var targetType = symbol is IMethodSymbol methodSym ? methodSym.ContainingType : symbol as INamedTypeSymbol;
         if (targetType is null) return false;

         switch (targetType.Name)
         {
            case "MagicBytesAttribute" or "MagicBytes":
               kind = FramingProtocolGenerator.ProtocolAttributeKind.MagicBytes;
               return true;
            case "VersionFieldAttribute" or "VersionField":
               kind = FramingProtocolGenerator.ProtocolAttributeKind.VersionField;
               return true;
            case "ProtocolFieldAttribute" or "ProtocolField":
               kind = FramingProtocolGenerator.ProtocolAttributeKind.ProtocolField;
               return true;
            case "FlagsFieldAttribute" or "FlagsField":
               kind = FramingProtocolGenerator.ProtocolAttributeKind.FlagsField;
               return true;
            case "VarNumberFieldAttribute" or "VarNumberField":
               kind = FramingProtocolGenerator.ProtocolAttributeKind.VarNumberField;
               return true;
            case "ByteSequenceFieldAttribute" or "ByteSequenceField":
               kind = FramingProtocolGenerator.ProtocolAttributeKind.ByteSequenceField;
               return true;
            default:
               return false;
         }
      }
   }
}
