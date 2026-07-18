using Beskar.Mqtt.Server.Enums;

namespace Beskar.Mqtt.Server.Options;

/// <summary>
/// MQTT-related server options. This does not include lower-level options such as TCP / QUIC transport options.
/// </summary>
public sealed class MqttServerOptions
{
   /// <summary>
   /// All keep alive options.
   /// </summary>
   public MqttServerKeepAliveOptions KeepAlive { get; set; } = new();

   /// <summary>
   /// Whether persistent sessions are enbabled.
   /// </summary>
   public bool SupportPersistentSessions { get; set; }

   /// <summary>
   /// How to handle overflow of pending messages.
   /// </summary>
   public MessageOverflowBehavior PendingMessageOverflowBehavior { get; set; } = MessageOverflowBehavior.DropOldest;

   /// <summary>
   /// Maximum number of pending messages per connection.
   /// This should never be set to a value that is too low, as it may cause the server to drop messages.
   /// But also this should never be set to a value that is too high, as it may cause problems.
   /// </summary>
   public ushort MaxPendingMessagesPerConnection { get; set; } = 1024;

   /// <summary>
   /// The maximum number of concurrent QoS 1 and QoS 2 messages in flight allowed for a client connection.
   /// </summary>
   public ushort ReceiveMaximum { get; set; } = 1000;

   /// <summary>
   /// Default timeout for packet awaits etc.
   /// </summary>
   public TimeSpan DefaultTimeout { get; set; } = TimeSpan.FromSeconds(120);

   /// <summary>
   /// The maximum topic alias value that the server allows clients to use.
   /// Defaults to 32.
   /// </summary>
   public ushort TopicAliasMaximum { get; set; } = 32;
}
