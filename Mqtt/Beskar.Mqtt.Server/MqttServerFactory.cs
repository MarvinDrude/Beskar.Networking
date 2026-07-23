using Beskar.Mqtt.Server.Options;

namespace Beskar.Mqtt.Server;

/// <summary>
/// A factory class for creating MQTT Server builders.
/// </summary>
public static class MqttServerFactory
{
   /// <summary>
   /// Creates a new <see cref="MqttServerBuilder"/> instance, optionally configured with the specified server options.
   /// </summary>
   /// <param name="options">The optional configuration options for the MQTT server.</param>
   /// <returns>A new <see cref="MqttServerBuilder"/> instance.</returns>
   public static MqttServerBuilder CreateBuilder(MqttServerOptions? options = null)
   {
      return new MqttServerBuilder(options);
   }
}
