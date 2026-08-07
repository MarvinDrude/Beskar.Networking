using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;

var endPoint = new IPEndPoint(IPAddress.Loopback, 1883);

// 1. Spin up MQTT Server / Broker
var broker = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(endPoint.Port)
   .Build();
var startResult = await broker.StartAsync();

// 2. Subscriber Client
await using var subClient = MqttClientFactory.CreateTcp();
subClient.AddMessageReceiveHandler((ctx, ct) => {
    Console.WriteLine($"Received [{ctx.Message.Topic}]: {Encoding.UTF8.GetString(ctx.Message.Payload.Span)}");
    return ValueTask.CompletedTask;
});
var connectResult = await subClient.ConnectAsync(new ConnectOptions { EndPoint = endPoint, ProtocolVersion = MqttProtocolVersion.V50 });
await subClient.SubscribeAsync(SubscribeOptions.Create().WithTopicFilter("sensors/temp", QualityOfServiceType.AtLeastOnce).Build());

// 3. Publisher Client
await using var pubClient = MqttClientFactory.CreateTcp();
var pubConnResult = await pubClient.ConnectAsync(new ConnectOptions { EndPoint = endPoint, ProtocolVersion = MqttProtocolVersion.V50 });
var pubResult = await pubClient.PublishAsync(PublishOptions.Create()
    .WithTopic("sensors/temp")
    .WithPayload("{ \"celsius\": 22.5 }")
    .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
    .Build());

await Task.Delay(500);

await subClient.DisconnectAsync(new DisconnectOptions());
await pubClient.DisconnectAsync(new DisconnectOptions());
await broker.StopAsync();
