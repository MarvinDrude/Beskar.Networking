namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// Specifies the supported MQTT protocol versions.
/// </summary>
public enum MqttProtocolVersion : byte
{
    /// <summary>
    /// MQTT version 3.1.
    /// </summary>
    V31 = 3,

    /// <summary>
    /// MQTT version 3.1.1.
    /// </summary>
    V311 = 4,

    /// <summary>
    /// MQTT version 5.0.
    /// </summary>
    V50 = 5
}
