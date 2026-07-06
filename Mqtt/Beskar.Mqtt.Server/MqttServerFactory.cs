using Beskar.Mqtt.Server.Options;

namespace Beskar.Mqtt.Server;

public static class MqttServerFactory
{
   public static MqttServerBuilder CreateBuilder(MqttServerOptions? options = null)
   {
      return new MqttServerBuilder(options);
   }
}
