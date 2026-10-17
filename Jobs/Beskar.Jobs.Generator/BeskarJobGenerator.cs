using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Beskar.Jobs.Generator;

[Generator]
public partial class BeskarJobGenerator : IIncrementalGenerator
{
   private const string AttributeFullName = "Beskar.Jobs.Common.Attributes.GeneratedJobHandlerAttribute";

   public void Initialize(IncrementalGeneratorInitializationContext context)
   {
      var typeDeclarations = context.SyntaxProvider
         .ForAttributeWithMetadataName(
            AttributeFullName,
            static (node, _) => IsSyntaxTargetForGeneration(node),
            static (ctx, token) => GetSemanticTargetForGeneration(ctx, token))
         .Where(static m => m is not null)
         .Select(static (m, _) => m!.Value);

      context.RegisterSourceOutput(typeDeclarations, GenerateSourceForType);
   }

   private static bool IsSyntaxTargetForGeneration(SyntaxNode node)
   {
      return node is TypeDeclarationSyntax { AttributeLists.Count: > 0 };
   }
}
