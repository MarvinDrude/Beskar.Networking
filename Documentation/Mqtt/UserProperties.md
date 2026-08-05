# MQTT v5 User Properties (Metadata Headers)

MQTT v5.0 introduces **User Properties**, which are custom key-value pairs (UTF-8 strings) that can be appended to almost any MQTT packet, including `CONNECT`, `PUBLISH`, `SUBSCRIBE`, and `DISCONNECT`.

User Properties function similarly to HTTP request headers. They allow you to attach application-specific metadata to a message without modifying the actual message payload, making it easier to route, trace, or filter messages.

---

## 1. Appending User Properties to a Publish Packet

To attach metadata to an outgoing message, use the `WithUserProperty` fluent builder methods on `PublishOptions`:

```csharp
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Protocol.Enums;

var telemetryPub = PublishOptions.Create()
   .WithTopic("sensor/telemetry")
   .WithPayload("{ \"temp\": 23.5 }")
   .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
   
   // Attach application metadata headers
   .WithUserProperty("traceparent", "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
   .WithUserProperty("device-id", "sensor-node-west-4")
   .WithUserProperty("client-version", "v1.2.0")
   .Build();

await mqttClient.PublishAsync(telemetryPub);
```

---

## 2. Reading User Properties from a Received Message

When a client receives a message, you can access the User Properties via the `Message.UserProperties` collection. This collection implements `IReadOnlyList<MqttUserProperty>`:

```csharp
using System.Text;

using var receiveToken = subscriberClient.AddMessageReceiveHandler((context, ct) =>
{
   var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
   Console.WriteLine($"Received payload: {payload}");

   // Process metadata headers
   foreach (var prop in context.Message.UserProperties)
   {
      var key = prop.Name;
      var valueStr = Encoding.UTF8.GetString(prop.Value.Span);
      Console.WriteLine($"Metadata -> {key}: {valueStr}");
   }

   return ValueTask.CompletedTask;
});
```

---

## 3. Common Use-Cases

### Distributed Tracing (APM)
You can inject W3C trace contexts (like `traceparent` and `tracestate`) into User Properties at the publisher, allowing Application Performance Monitoring (APM) systems to map message flows across different microservices asynchronously.

### Request-Response Pattern
By passing a `correlation-id` and a `reply-to` topic in the User Properties of a publish, a responder can process the query and send back a response to the specified response topic, carrying the same correlation ID so the requestor knows which request the reply belongs to.

### Client Capabilities & Routing
A broker interceptor or subscribing microservice can examine `UserProperties` (e.g. `client-version = v2`) to dynamically route payloads to matching processing pipelines or perform protocol compatibility checks.

---

## 4. Example Code Reference

Refer to the complete, runnable User Properties metadata demonstration:
- **Example Location**: [Program.cs](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Examples/Mqtt/Beskar.Mqtt.Example.UserProperties/Program.cs)
- **Features Verified**:
  - Broker setup and client subscription.
  - Attaching multiple custom User Properties to a publish.
  - Parsing and printing key-value pairs at the subscriber.
