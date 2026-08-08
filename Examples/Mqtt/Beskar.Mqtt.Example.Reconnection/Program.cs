using System.Net;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using Beskar.Networking.Abstractions.Backoffs;
using Beskar.Networking.Abstractions.Options;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;

Console.WriteLine();
Console.WriteLine("==========================================================");
Console.WriteLine(" MQTT Client Auto-Reconnection & Events Example           ");
Console.WriteLine("==========================================================");

const int Port = 8005;

// Build and configure the MQTT server
var mqttServer = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(Port)
   .Build();

TraceLogger.LogServerInfo("Server: Starting...");
var startResult = await mqttServer.StartAsync();
if (startResult.Failed)
{
   throw new InvalidOperationException($"Server failed to start: {startResult.Error.Detail}");
}
TraceLogger.LogServerInfo($"Server: Running on port {Port}.");

// Setup Client with Built-in Auto-Reconnection and Jittered Exponential Backoff
await using var mqttClient = MqttClientFactory.CreateTcp();

var connectOptions = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, Port))
   .WithProtocolVersion(MqttProtocolVersion.V50)
   .WithCleanSession()
   .WithAutoReconnect(new AutoReconnectOptions
   {
      IsEnabled = true,
      MaxRetryAttempts = 10,
      BackoffPolicy = new ExponentialBackoffPolicy(TimeSpan.FromSeconds(1)).WithFullJitter()
   })
   .Build();

// Register Client Event Handlers
using var connectedToken = mqttClient.AddConnectedHandler((context, ct) =>
{
   TraceLogger.LogClientInfo("Client Event: [Connected] Handshake complete.");
   return ValueTask.CompletedTask;
});

using var disconnectedToken = mqttClient.AddDisconnectedHandler((context, ct) =>
{
   TraceLogger.LogClientWarning($"Client Event: [Disconnected] ReasonCode = {context.ReasonCode}, BeforeConnected = {context.BeforeConnected}");
   return ValueTask.CompletedTask;
});

// Initial Connection
TraceLogger.LogInfo("\n--- Connecting Client Initially ---");
var connectResult = await mqttClient.ConnectAsync(connectOptions);
if (connectResult.Failed)
{
   throw new InvalidOperationException($"Client failed to connect: {connectResult.Error.Detail}");
}

await Task.Delay(500);

// Simulate Connection Loss by Stopping the Server
TraceLogger.LogInfo("\n--- Simulating Connection Loss (Stopping Server) ---");
await mqttServer.StopAsync();

// Wait to let the client detect connection loss and start reconnect attempts using backoff policy
await Task.Delay(3000);

// Restore Connection by Restarting the Server
TraceLogger.LogInfo("\n--- Simulating Recovery (Restarting Server) ---");
var restartResult = await mqttServer.StartAsync();
if (restartResult.Failed)
{
   throw new InvalidOperationException($"Server failed to restart: {restartResult.Error.Detail}");
}

// Wait for the client auto-reconnect loop to succeed
await Task.Delay(3000);

// Cleanup & Graceful Disconnect
TraceLogger.LogInfo("\n--- Performing Graceful Disconnect ---");
await mqttClient.DisconnectAsync(new DisconnectOptions());

await Task.Delay(500);

TraceLogger.LogServerInfo("Server: Stopping...");
await mqttServer.StopAsync();
TraceLogger.LogServerInfo("Server: Stopped.");

Console.WriteLine("==========================================================");
Console.WriteLine(" Reconnection Demo Finished Successfully.                 ");
Console.WriteLine("==========================================================");
