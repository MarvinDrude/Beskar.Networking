using System.Reflection;
using Beskar.Mqtt.Common.Generators;
using Microsoft.CodeAnalysis;
using Microsoft.CodeAnalysis.CSharp;

namespace Beskar.Mqtt.Common.Tests.Internal;

public sealed class MqttTopicGeneratorDiagnosticsAndTypesTests
{
   private static readonly Lazy<Type> GeneratorType = new(() =>
   {
      var generatorPath = Path.Combine(AppContext.BaseDirectory, "Beskar.Mqtt.Common.Generators.dll");
      var assembly = global::System.Reflection.Assembly.LoadFrom(generatorPath);
      return assembly.GetType("Beskar.Mqtt.Common.Generators.MqttTopicGenerator", throwOnError: true)!;
   });

   private static ISourceGenerator CreateTopicGenerator()
   {
      var instance = Activator.CreateInstance(GeneratorType.Value)!;
      if (instance is ISourceGenerator sg)
      {
         return sg;
      }

      if (instance is IIncrementalGenerator ig)
      {
         return ig.AsSourceGenerator();
      }

      throw new InvalidOperationException("Loaded MqttTopicGenerator does not implement a Roslyn generator interface.");
   }

   private static (Compilation OutputCompilation, GeneratorDriverRunResult RunResult) RunGenerator(string source)
   {
      var syntaxTree = CSharpSyntaxTree.ParseText(source);

      var referencePaths = AppDomain.CurrentDomain.GetAssemblies()
         .Where(static a => !a.IsDynamic && !string.IsNullOrEmpty(a.Location))
         .Select(static a => a.Location)
         .Append(typeof(GeneratedMqttTopicAttribute).Assembly.Location)
         .Distinct(StringComparer.OrdinalIgnoreCase);

      var references = referencePaths
         .Select(static path => (MetadataReference)MetadataReference.CreateFromFile(path))
         .ToArray();

      var compilation = CSharpCompilation.Create(
         assemblyName: "TopicGeneratorTestAssembly",
         syntaxTrees: [syntaxTree],
         references: references,
         options: new CSharpCompilationOptions(OutputKind.DynamicallyLinkedLibrary));

      var driver = CSharpGeneratorDriver.Create(CreateTopicGenerator());
      driver = (CSharpGeneratorDriver)driver.RunGeneratorsAndUpdateCompilation(
         compilation,
         out var outputCompilation,
         out _);

      return (outputCompilation, driver.GetRunResult());
   }

   [Test]
   public async Task Generator_OnRecordStruct_ShouldEmitCompilablePartialRecordStruct()
   {
      const string source = """
         using System;
         using Beskar.Mqtt.Common.Generators;

         namespace TestTopics;

         public partial record struct DeviceTopic
         {
            [GeneratedMqttTopic("devices/{id}")]
            public static partial bool IsMatch(ReadOnlySpan<char> topic);
         }
         """;

      var (outputCompilation, _) = RunGenerator(source);
      var compileErrors = outputCompilation.GetDiagnostics()
         .Where(static d => d.Severity == DiagnosticSeverity.Error)
         .ToArray();

      await Assert.That(compileErrors).IsEmpty();
   }

   [Test]
   public async Task Generator_OnGenericContainingType_ShouldPreserveTypeParameters()
   {
      const string source = """
         using System;
         using Beskar.Mqtt.Common.Generators;

         namespace TestTopics;

         public partial class GenericTopicRouter<TKey>
         {
            [GeneratedMqttTopic("tenants/{tenant}/events")]
            public static partial bool IsMatch(ReadOnlySpan<char> topic);
         }
         """;

      var (outputCompilation, _) = RunGenerator(source);
      var compileErrors = outputCompilation.GetDiagnostics()
         .Where(static d => d.Severity == DiagnosticSeverity.Error)
         .ToArray();

      await Assert.That(compileErrors).IsEmpty();
   }

   [Test]
   [Arguments("")]
   [Arguments("sensors//temp")]
   [Arguments("/sensors/temp")]
   [Arguments("sensors/temp/")]
   [Arguments("sensors/{id")]
   [Arguments("sensors/node{id}/temp")]
   public async Task Generator_OnEmptyOrMalformedPattern_ShouldReportErrorDiagnostic(string pattern)
   {
      var escapedPattern = pattern.Replace("\\", "\\\\").Replace("\"", "\\\"");
      var source = $$"""
         using System;
         using Beskar.Mqtt.Common.Generators;

         namespace TestTopics;

         public partial class InvalidTopicMatcher
         {
            [GeneratedMqttTopic("{{escapedPattern}}")]
            public static partial bool IsMatch(ReadOnlySpan<char> topic);
         }
         """;

      var (_, runResult) = RunGenerator(source);
      var generatorErrors = runResult.Diagnostics
         .Where(static d => d.Severity == DiagnosticSeverity.Error)
         .ToArray();

      await Assert.That(generatorErrors).IsNotEmpty();
   }
}
