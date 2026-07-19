using Microsoft.CodeAnalysis;

namespace Beskar.Mqtt.Common.Generators;

public partial class MqttTopicGenerator
{
    private static readonly DiagnosticDescriptor ContainingTypeMustBePartial = new(
        id: "MQTTGEN001",
        title: "Containing type must be partial",
        messageFormat: "The type '{0}' containing the generated topic method '{1}' must be partial",
        category: "Design",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true
    );

    private static readonly DiagnosticDescriptor InvalidMultiLevelWildcard = new(
        id: "MQTTGEN002",
        title: "Invalid MQTT Wildcard",
        messageFormat: "Multi-level wildcard '#' must be the last segment of the pattern and stand alone",
        category: "Design",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true
    );

    private static readonly DiagnosticDescriptor InvalidSingleLevelWildcard = new(
        id: "MQTTGEN003",
        title: "Invalid MQTT Wildcard",
        messageFormat: "Single-level wildcard '+' must stand alone in its segment",
        category: "Design",
        defaultSeverity: DiagnosticSeverity.Error,
        isEnabledByDefault: true
    );
}
