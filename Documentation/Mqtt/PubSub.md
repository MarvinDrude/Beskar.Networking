# MQTT Publish and Subscribe

Beskar.Mqtt makes it easy to subscribe to topics, receive published messages in real-time,
and publish telemetry/data payload messages.

---

## 1. Initializing the Clients

Beskar.Mqtt client and server instances are created using their respective factories.
Below is a simple setup simulating an **IoT Temperature Sensor** (publisher) and an **IoT Dashboard** (subscriber).

### Registering the Message Handler

On the subscriber side, use `AddMessageReceiveHandler` to handle incoming messages.
The disposable return of AddMessageReceiveHandler will unsubscribe the event handler if ``Dispose`` is called.

```csharp
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Utilities.Tracing;

await using var subscriberClient = MqttClientFactory.CreateTcp();

// Register the handler to process incoming messages
using var receiveHandlerToken = subscriberClient.AddMessageReceiveHandler((context, ct) =>
{
   var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
   Console.WriteLine($"[Dashboard] Message received on topic '{context.Message.Topic}': {payload}");

   return ValueTask.CompletedTask;
});
```

---

## 2. Subscribing to Topics

To subscribe to topics on the broker, build a `SubscribeOptions` instance using the fluent
`SubscribeOptions.Create()` builder, and call `SubscribeAsync()`. You can reuse these options and even
clear them.

```csharp
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;

var subscribeOptions = SubscribeOptions.Create()
   .WithTopicFilter("iot/devices/temperature", QualityOfServiceType.AtLeastOnce)
   .Build();

var subResult = await subscriberClient.SubscribeAsync(subscribeOptions);
if (subResult.Failed)
{
   Console.WriteLine($"Subscription failed: {subResult.Error.Detail}");
}
```

---

## 3. Publishing Messages

To publish messages from a client, build a `PublishOptions` instance
using `PublishOptions.Create()`, and call `PublishAsync()`.

> [!IMPORTANT]
> **Use the Topic Source Generator for Maximum Performance**
> Instead of manually constructing topic strings, it is highly recommended to use
> the [MQTT Topic Source Generator](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Documentation/Mqtt/TopicGenerator.md). The generator outputs strongly-typed formatter
> helper methods (like `FormatTopicToBytes(...)`) that compile to allocation-free UTF-8 byte arrays, which can be passed directly to `WithTopic` for zero-allocation publishing.

```csharp
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Protocol.Enums;

var publishOptions = PublishOptions.Create()
   .WithTopic("iot/devices/temperature"u8)
   .WithPayload("{ \"temp\": 24.2, \"status\": \"Warning\" }")
   .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
   .Build();

var pubResult = await publisherClient.PublishAsync(publishOptions);
if (pubResult.Failed)
{
   Console.WriteLine($"Publish failed: {pubResult.Error.Detail}");
}
```

---

## 4. Unsubscribing & Disconnecting

When done, unsubscribe from topics using `UnsubscribeAsync()` and disconnect gracefully using `DisconnectAsync()`.

```csharp
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Builders.Disconnecting;

// Unsubscribe
var unsubscribeOptions = UnsubscribeOptions.Create()
   .WithTopicFilter("iot/devices/temperature"u8)
   .Build();

await subscriberClient.UnsubscribeAsync(unsubscribeOptions);

// Disconnect
await subscriberClient.DisconnectAsync(new DisconnectOptions());
await publisherClient.DisconnectAsync(new DisconnectOptions());
```

---

## 5. Complete Example

Below is the complete code for a self-contained local pub-sub demonstration:

```csharp
using System.Net;
using System.Text;
using System.Threading.Tasks;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;

const int Port = 8003;
const string Topic = "iot/devices/temperature";

// Start local MQTT Server
var mqttServer = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(Port)
   .Build();

await mqttServer.StartAsync();

// Initialize clients
await using var subscriberClient = MqttClientFactory.CreateTcp();
await using var publisherClient = MqttClientFactory.CreateTcp();

// 1. Setup message callback on Subscriber
using var receiveHandlerToken = subscriberClient.AddMessageReceiveHandler((context, ct) =>
{
   var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
   Console.WriteLine($"[Dashboard] Received message: {payload}");
   return ValueTask.CompletedTask;
});

// 2. Connect both clients
var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
   ProtocolVersion = MqttProtocolVersion.V50
};
await subscriberClient.ConnectAsync(connectOptions);
await publisherClient.ConnectAsync(connectOptions);

// 3. Subscribe
var subscribeOptions = SubscribeOptions.Create()
   .WithTopicFilter(Topic, QualityOfServiceType.AtLeastOnce)
   .Build();
await subscriberClient.SubscribeAsync(subscribeOptions);

// 4. Publish
var publishOptions = PublishOptions.Create()
   .WithTopic(Topic)
   .WithPayload("{ \"temp\": 22.5, \"status\": \"Normal\" }")
   .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
   .Build();
await publisherClient.PublishAsync(publishOptions);

// Wait briefly for delivery
await Task.Delay(200);

// 5. Unsubscribe & Disconnect
var unsubscribeOptions = UnsubscribeOptions.Create()
   .WithTopicFilter(Topic)
   .Build();
await subscriberClient.UnsubscribeAsync(unsubscribeOptions);

await subscriberClient.DisconnectAsync(new DisconnectOptions());
await publisherClient.DisconnectAsync(new DisconnectOptions());

// Stop Server
await mqttServer.StopAsync();
```
