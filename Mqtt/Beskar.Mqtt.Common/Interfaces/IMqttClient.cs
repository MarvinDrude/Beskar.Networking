using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;

namespace Beskar.Mqtt.Common.Interfaces;

public interface IMqttClient
{
   public Task SubscribeAsync(SubscribeOptions options, CancellationToken ct = default);

   public Task UnsubscribeAsync(UnsubscribeOptions options, CancellationToken ct = default);
}
