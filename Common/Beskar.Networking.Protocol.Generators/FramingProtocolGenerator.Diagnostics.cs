using Microsoft.CodeAnalysis;

namespace Beskar.Networking.Protocol.Generators;

public partial class FramingProtocolGenerator
{
   private static readonly DiagnosticDescriptor TargetTypeMustBePartial = new(
      "BESKARPROT001",
      "Target type must be partial",
      "The type '{0}' decorated with [GenerateFramingProtocol] must be partial",
      "Design",
      DiagnosticSeverity.Error,
      true
   );

   private static readonly DiagnosticDescriptor DuplicateOrderValue = new(
      "BESKARPROT002",
      "Duplicate Order value",
      "Property '{0}' has duplicate Order value {1} in type '{2}'",
      "Design",
      DiagnosticSeverity.Error,
      true
   );

   private static readonly DiagnosticDescriptor InvalidLengthProperty = new(
      "BESKARPROT003",
      "Length property not found",
      "Byte sequence property '{0}' references length property '{1}' which was not found on type '{2}'",
      "Design",
      DiagnosticSeverity.Error,
      true
   );
}
