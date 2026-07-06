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


}
