# MQTT Subscription Streaming & Managed Subscriptions

`Beskar.Mqtt` provides a modern, high-level interface for consuming MQTT topics as asynchronous pull streams (`IAsyncEnumerable<T>`), topic-scoped callbacks, and one-shot condition awaiters.

It eliminates the boilerplate of manual connection synchronization, global event demultiplexing, and fragile subscription tracking across reconnects.

---

## 1. Motivation: Why Stream MQTT Topics?

Traditional MQTT client consumption uses global push event callbacks (`AddMessageReceiveHandler`):
- **Timing issues**: You cannot subscribe until `client.IsConnected` is true, forcing you to wire up `OnClientConnected` handlers manually.
- **Reconnect drops**: When the connection drops and reconnects, broker subscriptions are lost unless you manually re-subscribe every topic.
- **Global demultiplexing**: All incoming broker messages hit a single callback, requiring manual topic string/span parsing and `switch`/`if` branching.
- **Flow control**: Callbacks execute directly on the client's packet receiver pipeline. If your business logic is slow, it can back up the entire network receive loop.

### The Modern Alternative: `IAsyncEnumerable<T>`
With `SubscribeStream`, you can consume topic messages using C# `await foreach`:

```csharp
await foreach (JobStatusUpdate update in client.SubscribeJsonStream<JobStatusUpdate>("jobs/JOB-101/status", ct: ct))
{
    Console.WriteLine($"Job progress: {update.Progress}% ({update.Status})");

    if (update.IsCompleted)
    {
        break; // Automatically cleans up channels and unsubscribes from the broker!
    }
}
```

---

## 2. Core Features & Capabilities

### 2.1 Asynchronous Pull Streaming (`await foreach`)
- **`SubscribeStream(topicFilter, [qos], [options], [ct])`**: Returns `IAsyncEnumerable<MqttPublishMessage>` yielding raw MQTT messages.
- **`SubscribeStream<T>(topicFilter, decoder, [qos], [options], [ct])`**: Returns `IAsyncEnumerable<T>` using a custom decoder delegate (`Func<ReadOnlyMemory<byte>, T>`) or an [`IMqttPayloadDecoder<T>`](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Mqtt/Beskar.Mqtt.Common/Serialization/IMqttPayloadDecoder.cs) implementation.
- **`SubscribeJsonStream<T>(topicFilter, [jsonOptions], [qos], [options], [ct])`**: Returns `IAsyncEnumerable<T>` deserializing JSON payloads with optional `JsonSerializerOptions` or source-generated `JsonTypeInfo<T>`.

### 2.2 Topic-Scoped Callbacks
If you prefer push callbacks over loops, use `SubscribeTopicAsync`:

```csharp
// Subscribes and routes only matching topic messages to this handler
await using var subscription = await client.SubscribeJsonTopicAsync<JobStatusUpdate>(
    "jobs/+/status",
    async (update, context, ct) =>
    {
        await ProcessJobUpdateAsync(update, ct);
    });

// Handler remains active across reconnects until disposed:
// await subscription.DisposeAsync(); -> cleans up and unsubscribes if no other listeners remain
```

### 2.3 One-Shot Condition Awaiter (`WaitForMessageAsync`)
Common in job workflows where you trigger an operation and simply want to await its completion:

```csharp
// Publish start command
await client.PublishAsync(startJobOptions);

// Await the job reaching a terminal condition with timeout
JobStatusUpdate finalResult = await client.WaitForJsonMessageAsync<JobStatusUpdate>(
    "jobs/JOB-101/status",
    predicate: update => update.IsCompleted || update.IsFailed,
    timeout: TimeSpan.FromMinutes(5),
    ct: ct);
```

---

## 3. Automatic Re-Subscription on Reconnection

One of the most powerful features of the managed subscription layer is **transparent reconnection survival**:

1. Subscriptions can be created **at any time**—even before `client.ConnectAsync()` is called or while the client is temporarily offline.
2. The internal `MqttSubscriptionManager` tracks all active topic filters and their highest requested QoS.
3. When the network drops and the client reconnects (either via built-in `AutoReconnect` or a manual `ConnectAsync`), the client's `OnClientConnected` event triggers an automatic, batched `SUBSCRIBE` packet to the broker containing all active filters.
4. **Your `await foreach` loop does NOT throw or abort!** It stays alive waiting on its channel, and new messages resume flowing as soon as the broker reconnects.

```
       Client Connected                   Connection Lost                     Client Reconnected
┌───────────────────────────────┐  ┌───────────────────────────────┐  ┌─────────────────────────────────┐
│ Stream: jobs/101/status       │  │ Stream: jobs/101/status       │  │ Stream: jobs/101/status         │
│ Status: Active                │  │ Status: Buffered / Waiting    │  │ Status: Auto-resubscribed!      │
│ Broker: Subscribed            │  │ Broker: Disconnected          │  │ Broker: Subscribed (batched SUB)│
└───────────────────────────────┘  └───────────────────────────────┘  └─────────────────────────────────┘
```

---

## 4. Reference Counting & Unsubscription Lifecycle

### "What happens if I unsubscribe on the same client somewhere else?"

`Beskar.Mqtt` uses a **thread-safe reference-counting mechanism per topic filter** (`TopicSubscriptionEntry`):

#### Scenario A: Multiple Streams / Listeners on the Same Topic Filter
Suppose two different components in your application listen to the same topic filter:
- **Component 1** runs: `client.SubscribeStream("devices/+/status")`
- **Component 2** runs: `client.SubscribeStream("devices/+/status")`

1. **On First Listener**: The client sends a single `SUBSCRIBE` packet for `"devices/+/status"` to the broker.
2. **On Second Listener**: The client increments the reference count to `2`. Both streams receive matching messages.
   - **Automatic QoS Upgrade**: If Listener 1 subscribed at `QoS 0` and Listener 2 requests `QoS 2`, `MqttSubscriptionManager` automatically upgrades the broker subscription to `QoS 2`.
3. **When Component 1 finishes or breaks out of its loop**:
   - Component 1's stream is disposed.
   - The reference count drops from `2` to `1`.
   - **No `UNSUBSCRIBE` packet is sent to the broker!** Component 2 continues receiving messages uninterrupted.
4. **When Component 2 also finishes**:
   - The reference count drops from `1` to `0`.
   - The manager removes the topic entry and immediately sends a single `UNSUBSCRIBE` packet to the broker.

> [!TIP]
> **Pre-Connection Subscriptions**: You can register streams and topic handlers even **before** calling `client.ConnectAsync(...)`. When the client connects, all registered topics are automatically subscribed at their required QoS levels in a single optimized `SUBSCRIBE` packet.

#### Scenario B: What if user code calls low-level `client.UnsubscribeAsync(...)` directly?
The low-level `client.UnsubscribeAsync(new UnsubscribeOptions { ... })` method sends an `UNSUBSCRIBE` packet directly to the broker over the raw control stream.

- If you manually call `client.UnsubscribeAsync("devices/+/status")` while active streams are still listening:
  - The broker will acknowledge the unsubscription and stop delivering messages to the client.
  - The local `await foreach` streams remain open and will continue waiting on their channel (until cancelled or closed).
  - If the client subsequently disconnects and auto-reconnects, the internal `MqttSubscriptionManager` will re-synchronize its registered streams and **re-subscribe** the topic on the broker.
- **Best Practice Recommendation**: When using streaming or `SubscribeTopicAsync`, do **not** call `client.UnsubscribeAsync` manually. Instead, use natural C# scoping:
  - Exit the `await foreach` loop (`break`, `return`, or cancellation token).
  - Dispose the `IAsyncDisposable` token returned by `SubscribeTopicAsync`.
  This allows reference counting to cleanly manage broker unsubscriptions without interfering with concurrent consumers.

---

## 5. Configurable Payload Decoding Per Subscription

Decoding is configured individually on each stream or topic subscription, ensuring that different topics or different consumers on the same topic can deserialize payloads differently.

### 5.1 Using a Lambda / Delegate
Pass a simple decoding delegate:

```csharp
await foreach (double temp in client.SubscribeStream(
    "sensors/temp",
    decoder: bytes => BitConverter.ToDouble(bytes.Span),
    ct: ct))
{
    Console.WriteLine($"Temp: {temp}°C");
}
```

### 5.2 Using JSON
Use the high-level JSON extensions:

```csharp
var jsonOptions = new JsonSerializerOptions { PropertyNamingPolicy = JsonNamingPolicy.CamelCase };

await foreach (JobProgressUpdate update in client.SubscribeJsonStream<JobProgressUpdate>(
    "jobs/+/progress",
    jsonOptions: jsonOptions,
    ct: ct))
{
    // ...
}
```

### 5.3 Using Custom Binary Formats (MessagePack, Protobuf, MemoryPack)
Implement [`IMqttPayloadDecoder<T>`](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Mqtt/Beskar.Mqtt.Common/Serialization/IMqttPayloadDecoder.cs):

```csharp
using MessagePack;
using Beskar.Mqtt.Common.Serialization;

public sealed class MessagePackPayloadDecoder<T>(MessagePackSerializerOptions? options = null) : IMqttPayloadDecoder<T>
{
    public T Decode(ReadOnlyMemory<byte> payload) => MessagePackSerializer.Deserialize<T>(payload, options);
}

// Pass to stream:
await foreach (var telemetry in client.SubscribeStream(
    "sensors/+/telemetry",
    decoder: new MessagePackPayloadDecoder<SensorTelemetry>(),
    ct: ct))
{
    // ...
}
```

### 5.4 Error Isolation
If an incoming message has a corrupted or malformed payload, the decoder's exception is caught and logged via `TraceLogger.LogClientError`.
- It does **not** crash the client's packet receiver loop.
- It does **not** disrupt other concurrent streams or consumers.

---

## 6. Backpressure & Channel Buffer Configuration

Each stream subscription is powered by a `System.Threading.Channels.Channel<T>`. You can customize buffering and backpressure behavior using [`StreamSubscriptionOptions`](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Mqtt/Beskar.Mqtt.Common/Options/StreamSubscriptionOptions.cs):

```csharp
var options = new StreamSubscriptionOptions
{
    // Maximum number of unread items buffered before backpressure applies
    BoundedCapacity = 500,

    // Policy when buffer is full:
    // - BoundedChannelFullMode.Wait (default): Asynchronously waits for reader to consume space
    // - BoundedChannelFullMode.DropOldest: Drops oldest item to preserve real-time freshest status
    // - BoundedChannelFullMode.DropWrite: Drops the incoming item
    FullMode = BoundedChannelFullMode.DropOldest
};

await foreach (var status in client.SubscribeJsonStream<DeviceStatus>("devices/+/status", options: options, ct: ct))
{
    // ...
}
```

> [!TIP]
> For real-time telemetry or status updates where only the latest state matters, setting `FullMode = BoundedChannelFullMode.DropOldest` guarantees that slow UI or consumer threads will never accumulate outdated data or exhaust memory.

---

## 7. Runnable Examples

See the complete runnable sample projects in the repository:
- [**JSON Async Streaming Example**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Mqtt/Beskar.Mqtt.Example.JsonStreaming): Shows multi-stage job progress streaming, `await foreach` loop exit cleanup, and `WaitForJsonMessageAsync`.
- [**MessagePack Custom Decoder Example**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Mqtt/Beskar.Mqtt.Example.MessagePackStreaming): Shows binary MessagePack telemetry decoding across wildcards and alert interception.
