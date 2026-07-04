namespace Beskar.Mqtt.Client.States;

public readonly record struct MqttClientDisconnectReason(
   bool WasByClient, int ReasonCode);
