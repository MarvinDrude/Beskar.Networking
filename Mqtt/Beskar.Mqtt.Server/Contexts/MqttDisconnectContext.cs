using Beskar.Mqtt.Client.States;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server.Enums;
using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Contexts;

public sealed class MqttDisconnectContext
{
   public required MqttServerClient ServerClient { get; init; }

   public required DisconnectReasonCode Reason { get; init; }

   public required ClientDisconnectKind DisconnectKind { get; init; }

   public required bool IsSessionTakenOver { get; init; }
}
