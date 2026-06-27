using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Common.Interfaces;

public interface IMqttCredentialsProvider
{
   /// <summary>
   /// Get the credentials sent to the MQTT server as username and password.
   /// </summary>
   public ValueTask<MqttCredentials> GetCredentialsAsync(ConnectOptions options, CancellationToken cancellationToken = default);
}
