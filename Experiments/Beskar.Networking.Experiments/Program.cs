using System.Net;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
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

while (true)
{
   await Task.Delay(TimeSpan.FromHours(24));
}
