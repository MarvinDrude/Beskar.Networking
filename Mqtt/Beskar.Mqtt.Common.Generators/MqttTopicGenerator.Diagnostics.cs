using Microsoft.CodeAnalysis;

namespace Beskar.Mqtt.Common.Generators;

public partial class MqttTopicGenerator
{
   private static readonly DiagnosticDescriptor ContainingTypeMustBePartial = new(
      "MQTTGEN001",
      "Containing type must be partial",
      "The type '{0}' containing the generated topic method '{1}' must be partial",
      "Design",
      DiagnosticSeverity.Error,
      true
   );

   private static readonly DiagnosticDescriptor InvalidMultiLevelWildcard = new(
      "MQTTGEN002",
      "Invalid MQTT Wildcard",
      "Multi-level wildcard '#' must be the last segment of the pattern and stand alone",
      "Design",
      DiagnosticSeverity.Error,
      true
   );

   private static readonly DiagnosticDescriptor InvalidSingleLevelWildcard = new(
      "MQTTGEN003",
      "Invalid MQTT Wildcard",
      "Single-level wildcard '+' must stand alone in its segment",
      "Design",
      DiagnosticSeverity.Error,
      true
   );
}
