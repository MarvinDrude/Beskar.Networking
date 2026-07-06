namespace Beskar.Mqtt.Server.Options;

/// <summary>
/// All options for the MQTT server keep-alive logic.
/// </summary>
public sealed class MqttServerKeepAliveOptions
{
   /// <summary>
   /// The interval at which the server will check the keep alive states of all connected clients.
   /// </summary>
   public TimeSpan Interval { get; set; }
}
