
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Networking.Abstractions.Comparers;

namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttSession : IAsyncDisposable
{
   public DateTimeOffset? DisconnectionTimestamp { get; internal set; }

   public Dictionary<byte[], MqttSessionSubscription> Subscriptions { get; } = new(ByteArrayEqualityComparer.Instance);
}
