# MQTT Auto-Reconnection & Client Events

In real-world scenarios, network connections are unstable. A production-ready MQTT client must monitor its connection status using client events and perform automatic reconnection when disconnected unexpectedly.

---

## 1. Subscribing to Client Events

Beskar.Mqtt provides client-side lifecycle callbacks that you can register using fluent handler registration:

- `AddConnectedHandler`: Triggered when the initial or subsequent connection handshake completes successfully.
- `AddDisconnectedHandler`: Triggered whenever the client transitions to a disconnected state.

```csharp
using Beskar.Mqtt.Client;

await using var mqttClient = MqttClientFactory.CreateTcp();

// Connection established
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

## 2. Implementing Robust Auto-Reconnection

When implementing auto-reconnect, it is critical to:
1. **Distinguish intentional disconnects** (user clicked disconnect) from **unintentional disconnections** (network loss).
2. **Prevent parallel reconnection tasks** (cascading loops) since a failed reconnect attempt triggers the disconnected event again.

This is achieved using a graceful disconnect flag and a thread-safe reconnection lock:

```csharp
bool isGracefulDisconnect = false;
bool isReconnecting = false;
object reconnectLock = new();

using var disconnectedToken = mqttClient.AddDisconnectedHandler((context, ct) =>
{
   // 1. Ignore if we intentionally disconnected the client
   if (!isGracefulDisconnect)
   {
      lock (reconnectLock)
      {
         // 2. Prevent spawning another loop if one is already running
         if (isReconnecting) return ValueTask.CompletedTask;
         isReconnecting = true;
      }

      Console.WriteLine("Connection lost unexpectedly! Starting reconnect loop...");

      _ = Task.Run(async () =>
      {
         try
         {
            int attempt = 0;
            while (true)
            {
               attempt++;
               Console.WriteLine($"Attempting reconnect #{attempt}...");

               var result = await mqttClient.ConnectAsync(connectOptions);
               if (!result.Failed)
               {
                  Console.WriteLine("Reconnected successfully!");
                  break;
               }

               Console.WriteLine($"Reconnect failed: {result.Error.Detail}. Retrying in 1.5s...");
               await Task.Delay(1500);
            }
         }
         finally
         {
            lock (reconnectLock)
            {
               isReconnecting = false;
            }
         }
      });
   }

   return ValueTask.CompletedTask;
});
```

---

## 3. Complete Simulation Example

The following code starts a local server, connects a client, shuts down the server to trigger an unexpected disconnect, restarts the server, and verifies the client successfully reconnects:

```csharp
using System.Net;
using System.Threading.Tasks;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;

const int Port = 8005;

// Start Server
var mqttServer = MqttServerFactory.CreateBuilder()
   .UseTcp(Port)
   .Build();
await mqttServer.StartAsync();

// Setup Client
await using var mqttClient = MqttClientFactory.CreateTcp();
var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
   ProtocolVersion = MqttProtocolVersion.V50
};

// Auto-Reconnect Logic
bool isGracefulDisconnect = false;
bool isReconnecting = false;
object reconnectLock = new();

using var connectedToken = mqttClient.AddConnectedHandler((context, ct) =>
{
   Console.WriteLine("Client Event: [Connected] Handshake complete.");
   return ValueTask.CompletedTask;
});

using var disconnectedToken = mqttClient.AddDisconnectedHandler((context, ct) =>
{
   Console.WriteLine($"Client Event: [Disconnected] ReasonCode = {context.ReasonCode}");

   if (!isGracefulDisconnect)
   {
      lock (reconnectLock)
      {
         if (isReconnecting) return ValueTask.CompletedTask;
         isReconnecting = true;
      }

      Console.WriteLine("Client Event: Connection lost unexpectedly! Starting auto-reconnect loop...");

      _ = Task.Run(async () =>
      {
         try
         {
            int attempt = 0;
            while (true)
            {
               attempt++;
               Console.WriteLine($"Client Event: Attempting reconnect #{attempt}...");

               var result = await mqttClient.ConnectAsync(connectOptions);
               if (!result.Failed)
               {
                  Console.WriteLine("Client Event: Reconnected successfully!");
                  break;
               }

               Console.WriteLine($"Client Event: Reconnect failed. Retrying in 1.5 seconds...");
               await Task.Delay(1500);
            }
         }
         finally
         {
            lock (reconnectLock)
            {
               isReconnecting = false;
            }
         }
      });
   }
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

// Graceful Disconnect
isGracefulDisconnect = true;
await mqttClient.DisconnectAsync(new DisconnectOptions());
await Task.Delay(500);

await mqttServer.StopAsync();
```
