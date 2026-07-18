# MQTT Quality of Service (QoS) Levels

MQTT defines three Quality of Service (QoS) levels to control the guarantee
of message delivery between clients and the broker. Beskar.Mqtt supports all three
levels for both publishing and subscribing.

---

## The Three QoS Levels

| QoS Level | Name | Guarantee | Overhead | Handshake Steps |
| :--- | :--- | :--- | :--- | :--- |
| **QoS 0** | At Most Once | Fire-and-forget; message is sent once and may be lost if network drops. | Lowest | 1 (No acknowledgment) |
| **QoS 1** | At Least Once | Message is guaranteed to arrive, but duplicates may occur. | Medium | 2 (PUBLISH -> PUBACK) |
| **QoS 2** | Exactly Once | Message is guaranteed to arrive exactly once, with zero duplicates. | Highest | 4 (PUBLISH -> PUBREC -> PUBREL -> PUBCOMP) |

---

## 1. Subscribing to QoS Levels

When subscribing to a topic, you specify the **maximum** QoS level you wish to receive.
If a publisher publishes with a lower QoS, the broker delivers at that lower QoS.

```csharp
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;

// Subscribe requesting Exactly Once (QoS 2) delivery
var subscribeOptions = SubscribeOptions.Create()
   .WithTopicFilter("sensors/telemetry", QualityOfServiceType.ExactlyOnce)
   .Build();

await subscriberClient.SubscribeAsync(subscribeOptions);
```

---

## 2. Publishing at Different QoS Levels

Use `PublishOptions.Create()` to build the publish packet with the desired QoS level.

### QoS 0 (At Most Once)
```csharp
var pubOptions0 = PublishOptions.Create()
   .WithTopic("sensors/telemetry")
   .WithPayload("QoS 0 payload")
   .WithQualityOfService(QualityOfServiceType.AtMostOnce)
   .Build();

await publisherClient.PublishAsync(pubOptions0);
```

### QoS 1 (At Least Once)
```csharp
var pubOptions1 = PublishOptions.Create()
   .WithTopic("sensors/telemetry")
   .WithPayload("QoS 1 payload")
   .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
   .Build();

await publisherClient.PublishAsync(pubOptions1);
```

### QoS 2 (Exactly Once)
```csharp
var pubOptions2 = PublishOptions.Create()
   .WithTopic("sensors/telemetry")
   .WithPayload("QoS 2 payload")
   .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
   .Build();

await publisherClient.PublishAsync(pubOptions2);
```

---

## 3. Complete Example

Below is a complete self-contained example demonstrating all three QoS levels on a local server instance:

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

const int Port = 8004;

// Start local MQTT Server
var mqttServer = MqttServerFactory.CreateBuilder()
   .UseTcp(Port)
   .Build();
await mqttServer.StartAsync();

// Initialize clients
await using var subscriberClient = MqttClientFactory.CreateTcp();
await using var publisherClient = MqttClientFactory.CreateTcp();

// Setup message callback on Subscriber
using var receiveHandlerToken = subscriberClient.AddMessageReceiveHandler((context, ct) =>
{
   var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
   Console.WriteLine($"[Subscriber] Received '{payload}' on '{context.Message.Topic}' with QoS {context.Message.QualityOfService}");
   return ValueTask.CompletedTask;
});

// Connect
var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
   ProtocolVersion = MqttProtocolVersion.V50
};
await subscriberClient.ConnectAsync(connectOptions);
await publisherClient.ConnectAsync(connectOptions);

// Subscribe with QoS 2 to receive all QoS levels
var subscribeOptions = SubscribeOptions.Create()
   .WithTopicFilter("qos/#", QualityOfServiceType.ExactlyOnce)
   .Build();
await subscriberClient.SubscribeAsync(subscribeOptions);

// Publish QoS 0
var pubOptions0 = PublishOptions.Create()
   .WithTopic("qos/level0")
   .WithPayload("QoS 0 Message")
   .WithQualityOfService(QualityOfServiceType.AtMostOnce)
   .Build();
await publisherClient.PublishAsync(pubOptions0);

// Publish QoS 1
var pubOptions1 = PublishOptions.Create()
   .WithTopic("qos/level1")
   .WithPayload("QoS 1 Message")
   .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
   .Build();
await publisherClient.PublishAsync(pubOptions1);

// Publish QoS 2
var pubOptions2 = PublishOptions.Create()
   .WithTopic("qos/level2")
   .WithPayload("QoS 2 Message")
   .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
   .Build();
await publisherClient.PublishAsync(pubOptions2);

// Wait briefly for delivery
await Task.Delay(300);

// Unsubscribe and Disconnect
var unsubscribeOptions = UnsubscribeOptions.Create()
   .WithTopicFilter("qos/#")
   .Build();
await subscriberClient.UnsubscribeAsync(unsubscribeOptions);

await subscriberClient.DisconnectAsync(new DisconnectOptions());
await publisherClient.DisconnectAsync(new DisconnectOptions());

await mqttServer.StopAsync();
```
