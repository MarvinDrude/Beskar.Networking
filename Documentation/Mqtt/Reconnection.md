# MQTT Auto-Reconnection & Client Events

In real-world scenarios, network connections are unstable. `Beskar.Mqtt` provides **built-in automatic reconnection**
with pluggable backoff policies (`IBackoffPolicy`, jitter support, retry attempt caps) directly configurable via `ConnectOptions`.

---

## 1. Built-in Auto-Reconnection & Backoff Policies

`MqttClient` handles connection recovery automatically upon ungraceful connection loss. You can configure retry limits
and backoff behavior using `AutoReconnectOptions` on `ConnectOptionsBuilder`:

```csharp
using System.Net;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Networking.Abstractions.Backoffs;
using Beskar.Networking.Abstractions.Options;

await using var mqttClient = MqttClientFactory.CreateTcp();

var connectOptions = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, 1883))
   .WithCleanSession()
   .WithAutoReconnect(new AutoReconnectOptions
   {
      IsEnabled = true,
      MaxRetryAttempts = 10,
      BackoffPolicy = new ExponentialBackoffPolicy(TimeSpan.FromSeconds(1)).WithFullJitter()
   })
   .Build();

await mqttClient.ConnectAsync(connectOptions);
```

### Supported Backoff Strategies
- **`ExponentialBackoffPolicy`**: Exponentially increases delays ($2^n \times \text{initial}$).
- **`LinearBackoffPolicy`**: Linearly increases delays by a fixed increment per attempt.
- **`ConstantBackoffPolicy`**: Fixed interval between attempts.
- **`DecorrelatedJitterBackoffPolicy`**: AWS decorrelated jitter algorithm.
- **Fluent Jitter Extensions**: `.WithJitter()`, `.WithFullJitter()`, `.WithEqualJitter()` decorators to prevent thundering herd reconnection spikes.

---

## 2. Subscribing to Client Events

Beskar.Mqtt provides client-side lifecycle callbacks that you can register using fluent handler registration:

- `AddConnectingHandler`: Triggered when client starts connecting or auto-reconnecting.
- `AddConnectedHandler`: Triggered when the initial or subsequent connection handshake completes successfully.
- `AddDisconnectedHandler`: Triggered whenever the client transitions to a disconnected state.

```csharp
using Beskar.Mqtt.Client;

await using var mqttClient = MqttClientFactory.CreateTcp();

// Connection established / reconnected
using var connectedToken = mqttClient.AddConnectedHandler((context, ct) =>
{
   Console.WriteLine("Client connected successfully.");
   return ValueTask.CompletedTask;
});

// Connection terminated
using var disconnectedToken = mqttClient.AddDisconnectedHandler((context, ct) =>
{
   Console.WriteLine($"Client disconnected. Reason: {context.ReasonCode}");
   return ValueTask.CompletedTask;
});
```

---

## 3. Complete Simulation Example

The following example spins up a local broker, connects an `MqttClient` with built-in auto-reconnect and jittered
exponential backoff, stops the server to simulate connection loss, restarts it, and verifies auto-reconnection:

```csharp
using System.Net;
using System.Threading.Tasks;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using Beskar.Networking.Abstractions.Backoffs;
using Beskar.Networking.Abstractions.Options;

const int Port = 8005;

// Start Server
var mqttServer = MqttServerFactory.CreateBuilder()
   .UseTcp(Port)
   .Build();
await mqttServer.StartAsync();

// Setup Client with Built-in Auto-Reconnect
await using var mqttClient = MqttClientFactory.CreateTcp();
var connectOptions = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, Port))
   .WithProtocolVersion(MqttProtocolVersion.V50)
   .WithAutoReconnect(new AutoReconnectOptions
   {
      IsEnabled = true,
      MaxRetryAttempts = 10,
      BackoffPolicy = new ExponentialBackoffPolicy(TimeSpan.FromSeconds(1)).WithFullJitter()
   })
   .Build();

using var connectedToken = mqttClient.AddConnectedHandler((context, ct) =>
{
   Console.WriteLine("Client Event: [Connected] Handshake complete.");
   return ValueTask.CompletedTask;
});

using var disconnectedToken = mqttClient.AddDisconnectedHandler((context, ct) =>
{
   Console.WriteLine($"Client Event: [Disconnected] ReasonCode = {context.ReasonCode}");
   return ValueTask.CompletedTask;
});

// Initial Connection
await mqttClient.ConnectAsync(connectOptions);
await Task.Delay(500);

// Stop Server (Simulate Loss)
await mqttServer.StopAsync();
await Task.Delay(3000);

// Restart Server (Simulate Recovery)
await mqttServer.StartAsync();
await Task.Delay(3000);

// Graceful Disconnect (Cancels Auto-Reconnect)
await mqttClient.DisconnectAsync(new DisconnectOptions());
await Task.Delay(500);

await mqttServer.StopAsync();
```
