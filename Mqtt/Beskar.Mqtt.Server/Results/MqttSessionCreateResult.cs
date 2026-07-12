using Beskar.Mqtt.Server.Internal;

namespace Beskar.Mqtt.Server.Results;

public struct MqttSessionCreateResult
{
   public required MqttSession Session { get; init; }

   public bool IsSessionPresent { get; set; }

   public bool IsSessionTakenOver { get; init; }
}
