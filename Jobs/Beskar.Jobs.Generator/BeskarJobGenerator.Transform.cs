using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Beskar.Jobs.Generator;

public partial class BeskarJobGenerator
{
   private static GeneratedJobHandlerModel? GetSemanticTargetForGeneration(
      GeneratorAttributeSyntaxContext context,
      CancellationToken cancellationToken)
   {
      if (context.TargetNode is not TypeDeclarationSyntax typeDeclaration) return null;
      if (context.TargetSymbol is not INamedTypeSymbol typeSymbol) return null;

      var attributeData = context.Attributes.FirstOrDefault();
      if (attributeData is null) return null;

      cancellationToken.ThrowIfCancellationRequested();

      var loc = typeDeclaration.Identifier.GetLocation();
      var typeLocation = new LocationModel(
         loc.SourceTree?.FilePath ?? "",
         loc.SourceSpan.Start,
         loc.SourceSpan.Length
      );

      var typeKindStr = typeSymbol.IsRecord
         ? (typeSymbol.IsValueType ? "record struct" : "record")
         : (typeSymbol.IsValueType ? "struct" : "class");

      var accessibility = typeSymbol.DeclaredAccessibility switch
      {
         Accessibility.Public => "public",
         Accessibility.Internal => "internal",
         Accessibility.Private => "private",
         Accessibility.Protected => "protected",
         Accessibility.ProtectedOrInternal => "protected internal",
         Accessibility.ProtectedAndInternal => "private protected",
         _ => "public"
      };

      var namespaceName = typeSymbol.ContainingNamespace.IsGlobalNamespace
         ? ""
         : typeSymbol.ContainingNamespace.ToDisplayString();

      var isTargetPartial = typeDeclaration.Modifiers.Any(SyntaxKind.PartialKeyword);
      var nonPartialTypeName = isTargetPartial ? "" : typeSymbol.Name;

      var nestingTypes = new List<NestingModel>();
      var currentType = typeSymbol.ContainingType;

      while (currentType is not null)
      {
         cancellationToken.ThrowIfCancellationRequested();

         var currentIsPartial = currentType.DeclaringSyntaxReferences
            .Select(r => r.GetSyntax(cancellationToken))
            .OfType<TypeDeclarationSyntax>()
            .Any(t => t.Modifiers.Any(SyntaxKind.PartialKeyword));

         if (!currentIsPartial && string.IsNullOrEmpty(nonPartialTypeName))
         {
            nonPartialTypeName = currentType.Name;
         }

         var nestingKind = currentType.IsRecord
            ? (currentType.IsValueType ? "record struct" : "record")
            : (currentType.IsValueType ? "struct" : "class");

         nestingTypes.Add(new NestingModel(currentType.Name, nestingKind));
         currentType = currentType.ContainingType;
      }

      nestingTypes.Reverse();

      var jobType = attributeData.ConstructorArguments.Length > 0
         ? attributeData.ConstructorArguments[0].Value as string ?? ""
         : "";

      var isInvalidJobType = string.IsNullOrWhiteSpace(jobType);

      var queue = "default";
      var isInvalidQueue = false;

      foreach (var namedArg in attributeData.NamedArguments)
      {
         if (namedArg.Key == "Queue")
         {
            var rawQueue = namedArg.Value.Value as string;
            if (string.IsNullOrWhiteSpace(rawQueue))
            {
               isInvalidQueue = true;
            }
            else
            {
               queue = rawQueue!;
            }
         }
      }

      var implementsJobHandler = false;
      foreach (var iface in typeSymbol.AllInterfaces)
      {
         if (iface.ContainingNamespace.ToDisplayString() == "Beskar.Jobs.Common.Interfaces" &&
             iface is { Name: "IJobHandler", TypeArguments.Length: 1 } or { Name: "IJobHandlerWithResult", TypeArguments.Length: 2 })
         {
            implementsJobHandler = true;
            break;
         }
      }

      return new GeneratedJobHandlerModel(
         typeSymbol.Name,
         typeKindStr,
         accessibility,
         namespaceName,
         [.. nestingTypes],
         jobType,
         queue,
         typeLocation,
         IsErrorTypeNotPartial: !string.IsNullOrEmpty(nonPartialTypeName),
         ErrorNonPartialTypeName: nonPartialTypeName,
         IsErrorInvalidJobType: isInvalidJobType,
         IsErrorInvalidQueue: isInvalidQueue,
         IsErrorMissingInterface: !implementsJobHandler
      );
   }
}
