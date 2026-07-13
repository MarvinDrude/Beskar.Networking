using System;
using System.Net;
using System.Reflection;
using Beskar.Memory.Results;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Server;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Transports.Common.Hosting;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Tcp.Extensions;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;
Console.WriteLine();

var mqttServer = MqttServerFactory.CreateBuilder()
   .UseTcp(8000)
   .UseWs(8001)
   .UseQuic(8002)
   .Build();

var result = await mqttServer.StartAsync();
if (result.Failed) throw new InvalidOperationException(result.Error.Detail);

var mqttClient = MqttClientFactory.CreateTcp();
var cresult = await mqttClient.ConnectAsync(new ConnectOptions()
{
   EndPoint = new IPEndPoint(IPAddress.Any, 8000)
});

await mqttClient.PingAsync();

while (true)
{
   await Task.Delay(TimeSpan.FromHours(24));
}
