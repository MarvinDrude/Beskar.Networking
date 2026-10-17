using System.Text;
using Beskar.Jobs.Example.Distributed.Common;
using Beskar.Mqtt.Server;

Console.WriteLine("=====================================================================");
Console.WriteLine($"  Beskar.Jobs Distributed Cluster — MQTT v5.0 Broker (TCP :{DistributedClusterConfig.BrokerPort})");
Console.WriteLine("=====================================================================");

var broker = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(DistributedClusterConfig.BrokerPort)
   .Build();

broker.Events.OnConnectIntercept.Add((ctx, _) =>
{
   Console.WriteLine($"[Broker {DateTime.Now:HH:mm:ss}] Client Connected:    {Encoding.UTF8.GetString(ctx.Client.ClientIdUtf8Bytes.Span)}");
   return ValueTask.CompletedTask;
});

broker.Events.OnDisconnect.Add((ctx, _) =>
{
   Console.WriteLine($"[Broker {DateTime.Now:HH:mm:ss}] Client Disconnected: {Encoding.UTF8.GetString(ctx.ServerClient.ClientIdUtf8Bytes.Span)}");
   return ValueTask.CompletedTask;
});

var startResult = await broker.StartAsync();
if (startResult.Failed)
{
   Console.WriteLine($"[Broker] Failed to start on port {DistributedClusterConfig.BrokerPort}: {startResult.Error.Detail}");
   return;
}

Console.WriteLine($"[Broker] Listening on {DistributedClusterConfig.BrokerEndPoint}. Press Enter or Ctrl+C to stop.");

using var cts = new CancellationTokenSource();
Console.CancelKeyPress += (_, e) =>
{
   e.Cancel = true;
   cts.Cancel();
};

try
{
   await Task.Delay(Timeout.Infinite, cts.Token);
}
catch (OperationCanceledException)
{
   // Shutting down
}

Console.WriteLine("[Broker] Stopping MQTT server...");
await broker.StopAsync();
