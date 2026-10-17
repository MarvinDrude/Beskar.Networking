using Microsoft.CodeAnalysis;

namespace Beskar.Jobs.Generator;

public partial class BeskarJobGenerator
{
   private static readonly DiagnosticDescriptor TargetTypeMustBePartial = new(
      "JOBGEN001",
      "Target or containing type must be partial",
      "The type '{0}' decorated with [GeneratedJobHandler] (and any containing types) must be declared partial",
      "Design",
      DiagnosticSeverity.Error,
      true
   );

   private static readonly DiagnosticDescriptor InvalidJobType = new(
      "JOBGEN002",
      "Invalid job type identifier",
      "The job type identifier specified on '{0}' cannot be empty or whitespace",
      "Design",
      DiagnosticSeverity.Error,
      true
   );

   private static readonly DiagnosticDescriptor InvalidQueueName = new(
      "JOBGEN003",
      "Invalid queue name",
      "The queue name specified on '{0}' cannot be empty or whitespace",
      "Design",
      DiagnosticSeverity.Error,
      true
   );

   private static readonly DiagnosticDescriptor MissingJobHandlerInterface = new(
      "JOBGEN004",
      "Missing job handler interface implementation",
      "The type '{0}' decorated with [GeneratedJobHandler] must implement IJobHandler<TArgs> or IJobHandlerWithResult<TArgs, TResult>",
      "Design",
      DiagnosticSeverity.Error,
      true
   );
}
