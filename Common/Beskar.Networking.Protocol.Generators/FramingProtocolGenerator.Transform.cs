using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Beskar.Networking.Protocol.Generators;

public partial class FramingProtocolGenerator
{
   private static GeneratedTypeModel? GetSemanticTargetForGeneration(
      GeneratorAttributeSyntaxContext context,
      CancellationToken cancellationToken)
   {
      if (context.TargetNode is not TypeDeclarationSyntax typeDeclaration) return null;
      if (context.TargetSymbol is not INamedTypeSymbol typeSymbol) return null;

      var isPartial = typeDeclaration.Modifiers.Any(SyntaxKind.PartialKeyword);
      cancellationToken.ThrowIfCancellationRequested();

      var loc = typeDeclaration.Identifier.GetLocation();
      var typeLocation = new LocationModel(
         loc.SourceTree?.FilePath ?? "",
         loc.SourceSpan.Start,
         loc.SourceSpan.Length
      );

      if (!isPartial)
      {
         return new GeneratedTypeModel(
            typeSymbol.Name,
            typeSymbol.IsValueType ? "struct" : "class",
            typeSymbol.ContainingNamespace.IsGlobalNamespace ? "" : typeSymbol.ContainingNamespace.ToDisplayString(),
            default,
            default,
            typeLocation,
            IsErrorTypeNotPartial: true,
            IsValueType: typeSymbol.IsValueType
         );
      }

      var nestingTypes = new List<NestingModel>();
      var currentType = typeSymbol.ContainingType;
      cancellationToken.ThrowIfCancellationRequested();

      while (currentType is not null)
      {
         var kind = currentType.IsRecord
            ? (currentType.IsValueType ? "record struct" : "record class")
            : (currentType.IsValueType ? "struct" : "class");
         nestingTypes.Add(new NestingModel(currentType.Name, kind));
         currentType = currentType.ContainingType;
      }
      nestingTypes.Reverse();

      var properties = new List<ProtocolPropertyModel>();
      cancellationToken.ThrowIfCancellationRequested();

      foreach (var member in typeSymbol.GetMembers())
      {
         if (member is not IPropertySymbol propSymbol) continue;

         AttributeData? protocolAttr = null;
         ProtocolAttributeKind kind = default;

         foreach (var attr in propSymbol.GetAttributes())
         {
            if (attr.AttributeClass.IsProtocolAttribute(out kind))
            {
               protocolAttr = attr;
               break;
            }
         }

         if (protocolAttr is null) continue;

         var order = 0;
         foreach (var namedArg in protocolAttr.NamedArguments)
         {
            if (namedArg is { Key: "Order", Value.Value: int orderVal })
            {
               order = orderVal;
            }
         }

         List<byte> magicBytes = [];
         if (kind == ProtocolAttributeKind.MagicBytes && protocolAttr.ConstructorArguments.Length > 0)
         {
            var arg0 = protocolAttr.ConstructorArguments[0];
            if (arg0.Kind == TypedConstantKind.Array)
            {
               foreach (var element in arg0.Values)
               {
                  if (element.Value is byte b) magicBytes.Add(b);
                  else if (element.Value is int i) magicBytes.Add((byte)i);
               }
            }
            else
            {
               foreach (var ctorArg in protocolAttr.ConstructorArguments)
               {
                  if (ctorArg.Value is byte b) magicBytes.Add(b);
                  else if (ctorArg.Value is int i) magicBytes.Add((byte)i);
               }
            }
         }

         string? lengthPropertyName = null;
         var safeCopyData = true;

         if (kind == ProtocolAttributeKind.ByteSequenceField)
         {
            if (protocolAttr.ConstructorArguments.Length > 0 && protocolAttr.ConstructorArguments[0].Value is string lenProp)
            {
               lengthPropertyName = lenProp;
            }
            if (protocolAttr.ConstructorArguments.Length > 1 && protocolAttr.ConstructorArguments[1].Value is bool copyValPos)
            {
               safeCopyData = copyValPos;
            }
            foreach (var namedArg in protocolAttr.NamedArguments)
            {
               if (namedArg is { Key: "safeCopyData", Value.Value: bool copyValNamed })
               {
                  safeCopyData = copyValNamed;
               }
            }
         }

         var propLoc = propSymbol.Locations.FirstOrDefault();
         var propLocationModel = new LocationModel(
            propLoc?.SourceTree?.FilePath ?? "",
            propLoc?.SourceSpan.Start ?? 0,
            propLoc?.SourceSpan.Length ?? 0
         );

         var isPropPartial = propSymbol.IsPartialDefinition;

         cancellationToken.ThrowIfCancellationRequested();
         properties.Add(new ProtocolPropertyModel(
            propSymbol.Name,
            propSymbol.Type.ToDisplayString(),
            kind,
            order,
            isPropPartial,
            [.. magicBytes],
            lengthPropertyName,
            safeCopyData,
            propLocationModel
         ));
      }

      properties.Sort((a, b) => a.Order.CompareTo(b.Order));

      cancellationToken.ThrowIfCancellationRequested();
      var typeKindStr = typeSymbol.IsRecord
         ? (typeSymbol.IsValueType ? "record struct" : "record class")
         : (typeSymbol.IsValueType ? "struct" : "class");

      return new GeneratedTypeModel(
         typeSymbol.Name,
         typeKindStr,
         typeSymbol.ContainingNamespace.IsGlobalNamespace ? "" : typeSymbol.ContainingNamespace.ToDisplayString(),
         [.. nestingTypes],
         [.. properties],
         typeLocation,
         IsErrorTypeNotPartial: false,
         IsValueType: typeSymbol.IsValueType
      );
   }
}
