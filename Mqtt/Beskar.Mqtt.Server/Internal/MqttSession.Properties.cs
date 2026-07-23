namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttSession
{
   /// <summary>
   /// Gets or sets the session expiry interval, defined in seconds.
   /// This property determines the duration for which the session state
   /// is preserved after the client disconnects. A value of 0 indicates
   /// that the session should be cleared immediately upon disconnection.
   /// Non-zero values specify the lifetime of the session state, allowing
   /// messages to be queued and delivered when the client reconnects.
   /// </summary>
   public uint ExpiryInterval { get; internal set; }

   /// <summary>
   /// Gets or sets the maximum number of unacknowledged QoS 1 or QoS 2 messages
   /// that the client is allowed to receive concurrently. This value determines the
   /// upper limit of in-flight messages sent to the client before receiving an acknowledgment.
   /// If the threshold is exceeded, the server may take appropriate action, such as
   /// disconnecting the client. A default value of 65535 indicates no explicit limit.
   /// </summary>
   public ushort ClientReceiveMaximum { get; internal set; } = 65535;

   /// <summary>
   /// Gets the client identifier represented as a UTF-8 encoded byte array.
   /// This property uniquely identifies a client within an MQTT session,
   /// enabling server-side operations such as session persistence,
   /// message routing, and client-specific event handling.
   /// </summary>
   public byte[] ClientIdUtf8Bytes { get; }
}
