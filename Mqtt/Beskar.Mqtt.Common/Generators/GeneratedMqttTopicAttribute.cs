
namespace Beskar.Mqtt.Common.Generators;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class GeneratedMqttTopicAttribute(string pattern) : Attribute
{
    public string Pattern { get; } = pattern;
}
