namespace Beskar.Mqtt.Protocol.Enums;

/// <summary>
/// MQTT v5.0 Property Identifier byte values.
/// </summary>
public enum PropertyIdentifier : byte
{
    /// <summary>
    /// Payload Format Indicator (0x01).
    /// <para>Data Type: Byte</para>
    /// </summary>
    PayloadFormatIndicator = 0x01,

    /// <summary>
    /// Message Expiry Interval (0x02).
    /// <para>Data Type: Four Byte Integer</para>
    /// </summary>
    MessageExpiryInterval = 0x02,

    /// <summary>
    /// Content Type (0x03).
    /// <para>Data Type: UTF-8 String</para>
    /// </summary>
    ContentType = 0x03,

    /// <summary>
    /// Response Topic (0x08).
    /// <para>Data Type: UTF-8 String</para>
    /// </summary>
    ResponseTopic = 0x08,

    /// <summary>
    /// Correlation Data (0x09).
    /// <para>Data Type: Binary Data</para>
    /// </summary>
    CorrelationData = 0x09,

    /// <summary>
    /// Subscription Identifier (0x0B).
    /// <para>Data Type: Variable Byte Integer</para>
    /// </summary>
    SubscriptionIdentifier = 0x0B,

    /// <summary>
    /// Session Expiry Interval (0x11).
    /// <para>Data Type: Four Byte Integer</para>
    /// </summary>
    SessionExpiryInterval = 0x11,

    /// <summary>
    /// Assigned Client Identifier (0x12).
    /// <para>Data Type: UTF-8 String</para>
    /// </summary>
    AssignedClientIdentifier = 0x12,

    /// <summary>
    /// Server Keep Alive (0x13).
    /// <para>Data Type: Two Byte Integer</para>
    /// </summary>
    ServerKeepAlive = 0x13,

    /// <summary>
    /// Authentication Method (0x15).
    /// <para>Data Type: UTF-8 String</para>
    /// </summary>
    AuthenticationMethod = 0x15,

    /// <summary>
    /// Authentication Data (0x16).
    /// <para>Data Type: Binary Data</para>
    /// </summary>
    AuthenticationData = 0x16,

    /// <summary>
    /// Request Problem Information (0x17).
    /// <para>Data Type: Byte</para>
    /// </summary>
    RequestProblemInformation = 0x17,

    /// <summary>
    /// Will Delay Interval (0x18).
    /// <para>Data Type: Four Byte Integer</para>
    /// </summary>
    WillDelayInterval = 0x18,

    /// <summary>
    /// Request Response Information (0x19).
    /// <para>Data Type: Byte</para>
    /// </summary>
    RequestResponseInformation = 0x19,

    /// <summary>
    /// Response Information (0x1A).
    /// <para>Data Type: UTF-8 String</para>
    /// </summary>
    ResponseInformation = 0x1A,

    /// <summary>
    /// Server Reference (0x1C).
    /// <para>Data Type: UTF-8 String</para>
    /// </summary>
    ServerReference = 0x1C,

    /// <summary>
    /// Reason String (0x1F).
    /// <para>Data Type: UTF-8 String</para>
    /// </summary>
    ReasonString = 0x1F,

    /// <summary>
    /// Receive Maximum (0x21).
    /// <para>Data Type: Two Byte Integer</para>
    /// </summary>
    ReceiveMaximum = 0x21,

    /// <summary>
    /// Topic Alias Maximum (0x22).
    /// <para>Data Type: Two Byte Integer</para>
    /// </summary>
    TopicAliasMaximum = 0x22,

    /// <summary>
    /// Topic Alias (0x23).
    /// <para>Data Type: Two Byte Integer</para>
    /// </summary>
    TopicAlias = 0x23,

    /// <summary>
    /// Maximum QoS (0x24).
    /// <para>Data Type: Byte</para>
    /// </summary>
    MaximumQos = 0x24,

    /// <summary>
    /// Retain Available (0x25).
    /// <para>Data Type: Byte</para>
    /// </summary>
    RetainAvailable = 0x25,

    /// <summary>
    /// User Property (0x26).
    /// <para>Data Type: UTF-8 String Pair</para>
    /// </summary>
    UserProperty = 0x26,

    /// <summary>
    /// Maximum Packet Size (0x27).
    /// <para>Data Type: Four Byte Integer</para>
    /// </summary>
    MaximumPacketSize = 0x27,

    /// <summary>
    /// Wildcard Subscription Available (0x28).
    /// <para>Data Type: Byte</para>
    /// </summary>
    WildcardSubscriptionAvailable = 0x28,

    /// <summary>
    /// Subscription Identifier Available (0x29).
    /// <para>Data Type: Byte</para>
    /// </summary>
    SubscriptionIdentifierAvailable = 0x29,

    /// <summary>
    /// Shared Subscription Available (0x2A).
    /// <para>Data Type: Byte</para>
    /// </summary>
    SharedSubscriptionAvailable = 0x2A
}
