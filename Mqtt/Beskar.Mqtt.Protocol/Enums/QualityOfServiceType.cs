namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// Quality of Service (QoS) levels for MQTT message delivery.
/// </summary>
public enum QualityOfServiceType : byte
{
    /// <summary>
    /// QoS 0: At most once delivery. The message is delivered according to the capabilities of the underlying network. No response is sent.
    /// </summary>
    AtMostOnce = 0,

    /// <summary>
    /// QoS 1: At least once delivery. The message is assured to arrive but duplicate messages can occur. Acknowledged via PUBACK.
    /// </summary>
    AtLeastOnce = 1,

    /// <summary>
    /// QoS 2: Exactly once delivery. The message is assured to arrive exactly once. Uses a four-step handshake (PUBLISH, PUBREC, PUBREL, PUBCOMP).
    /// </summary>
    ExactlyOnce = 2
}
