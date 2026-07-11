
namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttSession : IAsyncDisposable
{
   public DateTimeOffset? DisconnectionTimestamp { get; internal set; }
}
