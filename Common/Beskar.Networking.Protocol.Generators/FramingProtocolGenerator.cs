using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;

namespace Beskar.Networking.Protocol.Generators;

[Generator]
public partial class FramingProtocolGenerator : IIncrementalGenerator
{
   public void Initialize(IncrementalGeneratorInitializationContext context)
   {
      var typeDeclarations = context.SyntaxProvider
         .ForAttributeWithMetadataName(
            "Beskar.Networking.Protocol.Attributes.GenerateFramingProtocolAttribute",
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
