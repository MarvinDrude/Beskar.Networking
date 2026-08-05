# MQTT v5 Topic Aliases (Bandwidth Optimization)

In high-frequency messaging scenarios (such as telemetry updates or sensor feeds), long topic names like `vehicles/diagnostics/sensors/engine/temperature` are sent repeatedly. This introduces significant network bandwidth overhead relative to small payloads (e.g. `23.5 C`).

MQTT v5.0 introduces **Topic Aliases**, which allow clients and brokers to negotiate a mapping between long topic paths and short 2-byte integers. Once mapped, subsequent messages are published with the integer code instead of the full topic string, reducing header overhead.

---

## 1. Negotiation of Topic Aliases

During connection handshake, both the client and broker declare how many topic aliases they support using `TopicAliasMaximum`:

- **Client Connect**: Configures the maximum number of topic aliases the client is willing to accept from the server.
- **Server Connect**: Configures the maximum number of topic aliases the server is willing to accept from the client.

```csharp
// Client Connection setup
var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
   ProtocolVersion = MqttProtocolVersion.V50,
   
   // Enable topic aliases
   TopicAliasMaximum = 32
};
```

---

## 2. Using Topic Aliases to Send Messages

To use topic aliases, the publisher follows a two-stage process:

### Stage 1: Establish the Mapping (First Publish)
The publisher sends a `PUBLISH` packet containing **both** the full topic string **and** the topic alias identifier. This informs the broker to map `alias = 1` to `topic = "vehicles/diagnostics/sensors/engine/temperature"`.

```csharp
var pub1 = PublishOptions.Create()
   .WithTopic("vehicles/diagnostics/sensors/engine/temperature")
   .WithTopicAlias(1) // Map topic name to alias ID 1 on server
   .WithPayload("22.5 C (Initial Map)")
   .Build();

await publisherClient.PublishAsync(pub1);
```

### Stage 2: Send Optimized Publishes (Subsequent Publishes)
For all subsequent publishes to the same topic, the publisher sends a `PUBLISH` packet containing **only** the topic alias identifier. The topic path is left blank, saving network bytes:

```csharp
var pub2 = PublishOptions.Create()
   .WithTopicAlias(1) // Reference mapped alias ID 1 (Topic name left blank)
   .WithPayload("23.1 C (Optimized)")
   .Build();

await publisherClient.PublishAsync(pub2);
```

---

## 3. Receiving Aliased Messages

The broker translates the topic alias back to the original topic name before forwarding it to subscribers. 

> [!IMPORTANT]
> **Subscriber Transparency**
> Subscribers do **not** need to do any manual translation or alias matching. The broker automatically resolves topic aliases, ensuring that subscribers receive messages with the full topic path populated (e.g. `Message.Topic` contains the full string `"vehicles/diagnostics/sensors/engine/temperature"`).

---

## 4. Example Code Reference

Refer to the complete, runnable Topic Aliases bandwidth optimization demonstration:
- **Example Location**: [Program.cs](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Examples/Mqtt/Beskar.Mqtt.Example.TopicAliases/Program.cs)
- **Features Verified**:
  - Setting up the broker with custom `TopicAliasMaximum` options.
  - Creating publisher mapping via initial topic path and `TopicAlias = 1`.
  - Sending consecutive writes with only the `TopicAlias` property.
  - Subscriber automatically receiving the fully-resolved topic paths.
