using System;

namespace Beskar.Mqtt.Common.Generators;

[AttributeUsage(AttributeTargets.Method, AllowMultiple = false, Inherited = false)]
public sealed class GeneratedMqttTopicAttribute : Attribute
{
    public string Pattern { get; }

    public GeneratedMqttTopicAttribute(string pattern)
    {
        Pattern = pattern;
    }
}
