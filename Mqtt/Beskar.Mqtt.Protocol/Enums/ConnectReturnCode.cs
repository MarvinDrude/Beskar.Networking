namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// MQTT v3.1.1 Connection Return Codes (CONNACK return codes).
/// </summary>
public enum ConnectReturnCode : byte
{
    /// <summary>
    /// Connection Accepted (0).
    /// </summary>
    Accepted = 0,

    /// <summary>
    /// Connection Refused, unacceptable protocol version (1).
    /// </summary>
    UnacceptableProtocolVersion = 1,

    /// <summary>
    /// Connection Refused, identifier rejected (2).
    /// </summary>
    IdentifierRejected = 2,

    /// <summary>
    /// Connection Refused, server unavailable (3).
    /// </summary>
    ServerUnavailable = 3,

    /// <summary>
    /// Connection Refused, bad user name or password (4).
    /// </summary>
    BadUserNameOrPassword = 4,

    /// <summary>
    /// Connection Refused, not authorized (5).
    /// </summary>
    NotAuthorized = 5
}
