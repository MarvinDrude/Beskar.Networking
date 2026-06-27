namespace Beskar.Mqtt.Protocol.Models;

public readonly record struct MqttCredentials(
   string UserName,
   ReadOnlyMemory<byte> Password);
