using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Microsoft.CodeAnalysis.Text;

namespace Beskar.Mqtt.Common.Generators;

public partial class MqttTopicGenerator
{
   private static GeneratedMethodModel? GetSemanticTargetForGeneration(GeneratorSyntaxContext context,
      CancellationToken cancellationToken)
   {
      var methodDeclaration = (MethodDeclarationSyntax)context.Node;
      IMethodSymbol? methodSymbol = null;
      AttributeData? attributeData = null;

      foreach (var attributeList in methodDeclaration.AttributeLists)
      {
         foreach (var attribute in attributeList.Attributes)
         {
            var symbol = context.SemanticModel.GetSymbolInfo(attribute, cancellationToken).Symbol;
            if (symbol is IMethodSymbol attribMethodSymbol &&
                attribMethodSymbol.ContainingType.IsGeneratedMqttTopicAttribute())
            {
               methodSymbol = context.SemanticModel.GetDeclaredSymbol(methodDeclaration, cancellationToken);
               attributeData = methodSymbol?.GetAttributes()
                  .FirstOrDefault(ad => ad.AttributeClass.IsGeneratedMqttTopicAttribute());

               break;
            }
         }

         if (methodSymbol is not null) break;
      }

      if (methodSymbol is null || attributeData is null || attributeData.ConstructorArguments.Length == 0)
      {
         return null;
      }

      var patternValue = attributeData.ConstructorArguments[0].Value as string;
      if (string.IsNullOrEmpty(patternValue)) return null;

      var containingType = methodSymbol.ContainingType;
      if (containingType is null) return null;

      var isPartial = containingType.DeclaringSyntaxReferences
         .Select(r => r.GetSyntax(cancellationToken))
         .OfType<TypeDeclarationSyntax>()
         .Any(t => t.Modifiers.Any(SyntaxKind.PartialKeyword));

      var loc = methodDeclaration.Identifier.GetLocation();
      var methodLocation = new LocationModel(
         loc.SourceTree?.FilePath ?? "",
         loc.SourceSpan.Start,
         loc.SourceSpan.Length
      );

      var namespaceName = containingType.ContainingNamespace.IsGlobalNamespace
         ? ""
         : containingType.ContainingNamespace.ToDisplayString();

      var nestingTypes = new List<NestingModel>();
      var currentType = containingType;
      while (currentType is not null)
      {
         var typeKind = currentType.IsRecord ? "record" : currentType.IsValueType ? "struct" : "class";
         nestingTypes.Insert(0, new NestingModel(currentType.Name, typeKind));
         currentType = currentType.ContainingType;
      }

      var parameters = new List<ParameterModel>();
      foreach (var p in methodSymbol.Parameters)
      {
         var refKindStr = p.RefKind switch
         {
            RefKind.Out => "out",
            RefKind.Ref => "ref",
            RefKind.In => "in",
            _ => ""
         };

         parameters.Add(new ParameterModel(p.Name, p.Type.ToDisplayString(), refKindStr));
      }

      var modifiers = string.Join(" ", methodDeclaration.Modifiers.Select(m => m.Text));
      if (string.IsNullOrEmpty(modifiers)) modifiers = "public static partial";

      var isFormatter = methodSymbol.Name.StartsWith("Format", StringComparison.OrdinalIgnoreCase) ||
                        methodSymbol.Name.StartsWith("TryFormat", StringComparison.OrdinalIgnoreCase) ||
                        methodSymbol.Name.EndsWith("Formatter", StringComparison.OrdinalIgnoreCase);

      return new GeneratedMethodModel(
         MethodModifiers: modifiers,
         MethodName: methodSymbol.Name,
         ReturnType: methodSymbol.ReturnType.ToDisplayString(),
         NamespaceName: namespaceName,
         NestingTypes: nestingTypes.ToArray(),
         Parameters: parameters.ToArray(),
         Pattern: patternValue!,
         IsFormatter: isFormatter,
         MethodLocation: methodLocation,
         IsErrorContainingTypeNotPartial: !isPartial,
         ErrorContainingTypeName: containingType.Name
      );
   }

   private static void GenerateSourceForMethod(SourceProductionContext spc, GeneratedMethodModel model)
   {
      if (model.IsErrorContainingTypeNotPartial)
      {
         spc.ReportDiagnostic(Diagnostic.Create(
            ContainingTypeMustBePartial,
            model.MethodLocation.ToLocation(),
            model.ErrorContainingTypeName,
            model.MethodName));
         return;
      }

      if (!ValidatePattern(spc, model.MethodLocation, model.Pattern)) return;

      string generatedSource;
      if (model.IsFormatter)
      {
         generatedSource = GenerateFormatterMethod(model);
      }
      else
      {
         generatedSource = GenerateParserMethod(model);
      }

      if (string.IsNullOrEmpty(generatedSource)) return;

      var sourceBuilder = new StringBuilder();
      sourceBuilder.AppendLine("// <auto-generated/>");
      sourceBuilder.AppendLine("#nullable enable");
      sourceBuilder.AppendLine("#pragma warning disable CS0162 // Unreachable code detected");
      sourceBuilder.AppendLine("using System;");
      sourceBuilder.AppendLine("using System.Text;");
      sourceBuilder.AppendLine("using System.Buffers.Text;");
      sourceBuilder.AppendLine();

      var hasNamespace = !string.IsNullOrEmpty(model.NamespaceName);
      if (hasNamespace)
      {
         sourceBuilder.AppendLine($"namespace {model.NamespaceName}");
         sourceBuilder.AppendLine("{");
      }

      var indentLevel = hasNamespace ? 1 : 0;

      foreach (var type in model.NestingTypes)
      {
         sourceBuilder.AppendLine($"{new string(' ', indentLevel * 4)}partial {type.TypeKind} {type.Name}");
         sourceBuilder.AppendLine($"{new string(' ', indentLevel * 4)}{{");
         indentLevel++;
      }

      // Append the generated method body
      var methodLines = generatedSource.Split(["\r\n", "\n"], StringSplitOptions.None);
      foreach (var line in methodLines)
      {
         if (string.IsNullOrWhiteSpace(line))
         {
            sourceBuilder.AppendLine();
         }
         else
         {
            sourceBuilder.AppendLine($"{new string(' ', indentLevel * 4)}{line}");
         }
      }

      while (indentLevel > (hasNamespace ? 1 : 0))
      {
         indentLevel--;
         sourceBuilder.AppendLine($"{new string(' ', indentLevel * 4)}}}");
      }

      if (hasNamespace)
      {
         sourceBuilder.AppendLine("}");
      }

      var typePrefix = string.Join("_", model.NestingTypes.Select(t => t.Name));
      var hintName = $"{typePrefix}_{model.MethodName}.g.cs";

      spc.AddSource(hintName, SourceText.From(sourceBuilder.ToString(), Encoding.UTF8));
   }

   private static bool ValidatePattern(SourceProductionContext spc, LocationModel location, string pattern)
   {
      var segments = pattern.Split('/');

      for (var i = 0; i < segments.Length; i++)
      {
         var seg = segments[i];
         if (seg.Contains('#'))
         {
            // Multi-level wildcard must be the last segment and be exactly '#'
            if (seg != "#" || i != segments.Length - 1)
            {
               spc.ReportDiagnostic(Diagnostic.Create(
                  InvalidMultiLevelWildcard,
                  location.ToLocation()));
               return false;
            }
         }

         if (seg.Contains('+'))
         {
            // Single-level wildcard must stand alone
            if (seg != "+")
            {
               spc.ReportDiagnostic(Diagnostic.Create(
                  InvalidSingleLevelWildcard,
                  location.ToLocation()));
               return false;
            }
         }
      }

      return true;
   }
}
