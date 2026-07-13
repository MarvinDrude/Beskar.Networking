using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;
Console.WriteLine();

var mqttServer = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(8000)
   .UseWs(8001)
   .UseQuic(8002)
   .Build();

var result = await mqttServer.StartAsync();
if (result.Failed) throw new InvalidOperationException(result.Error.Detail);

var mqttClient = MqttClientFactory.CreateTcp();
var cresult = await mqttClient.ConnectAsync(new ConnectOptions()
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, 8000)
});

await mqttClient.PingAsync();

mqttClient.AddMessageReceiveHandler((ctx, ct) =>
{
   Console.WriteLine(Encoding.UTF8.GetString(ctx.Message.Payload.Span));
   return ValueTask.CompletedTask;
});

var sub = new SubscribeOptionsBuilder()
   .WithTopicFilter("test/2"u8, QualityOfServiceType.AtMostOnce)
   .WithTopicFilter("test/+/b"u8, QualityOfServiceType.AtMostOnce)
   //.WithTopicFilter("test/#"u8, QualityOfServiceType.ExactlyOnce)
   .WithUserProperty("test", "test")
   .Build();

var subAck = await mqttClient.SubscribeAsync(sub);

var pub = new PublishOptionsBuilder()
   .WithTopic("test/2"u8)
   .WithPayload("Test")
   .Build();

await mqttClient.PublishAsync(pub);

while (true)
{
   await Task.Delay(TimeSpan.FromHours(24));
}
