# MQTT Disconnection Safety & Message Persistence

In production environments, network partitions and hardware failures are common. Disconnection safety ensures that your MQTT broker and clients can recover gracefully without losing critical messages, state, or events.

This document describes how to implement robust disconnection safety in `Beskar.Networking` using both **server-side persistence (persistent sessions and offline queueing)** and **client-side fallback buffering (FIFO queues, last-message buffers, and JSON state persistence)**.

---

## 1. Server-Side Session Persistence & Offline Queueing

Persistent sessions allow the MQTT server to keep track of a client's state (subscriptions, in-flight messages, and unsent messages) even when the client is disconnected.

### Protocol Configuration

- **Clean Start (`CleanSession` / `CleanStart`)**:
  - `true`: The server discards any existing session state and creates a brand-new session.
  - `false`: The server attempts to restore an existing session for the client's `ClientId`.
- **Session Expiry Interval**:
  - Specifies how many seconds the server holds onto the session state after the client disconnects.
  - Set to `0` to expire the session immediately.
  - Set to `uint.MaxValue` to keep the session alive indefinitely until explicitly deleted.

### Configuring the Server

To support persistent sessions, the server must have `SupportPersistentSessions = true` enabled in its options:

```csharp
var options = new MqttServerOptions
{
   SupportPersistentSessions = true,
   // Limit to prevent unbounded memory growth
   MaxPendingMessagesPerConnection = 1024,
   // Drop behavior when queue exceeds limit
   PendingMessageOverflowBehavior = MessageOverflowBehavior.DropOldest
};

var mqttServer = MqttServerFactory.CreateBuilder()
   .UseTcp(Port)
   .Build(options);
```

### Server Offline Queueing Behavior

When a client with a persistent session goes offline:
1. The server retains its subscriptions.
2. Any new QoS 1 or QoS 2 messages published to matching topics are automatically queued in the client's internal offline queue.
3. Upon reconnection, the server automatically flushes the queue, delivering all pending messages in FIFO order to the client.

> [!NOTE]
> QoS 0 (At Most Once) messages are not queued during offline states as per the MQTT protocol specification. Use QoS 1 or QoS 2 for messages that must survive disconnections.

---

## 2. Server-Side Retained Messages JSON Save/Restore

Retained messages are stored by the broker to be delivered to new subscribers immediately. By default, these are kept in-memory and will be lost on broker restart. 

To make retained messages durable, you can hook into `OnRetainedMessageChanged` to serialize them to a JSON file, and use `OnLoadingRetainedMessages` to deserialize and load them at startup.

### Serialization Helper

```csharp
public static class ServerPersistenceHelper
{
   public static void SaveRetainedMessages(string path, IEnumerable<MqttPublishMessage> messages)
   {
      var dtos = messages.Select(m => new RetainedMessageDto
      {
         Topic = m.Topic,
         PayloadBase64 = Convert.ToBase64String(m.Payload.ToArray()),
         QualityOfService = (int)m.QualityOfService,
         Retain = m.Retain
      }).ToList();

      var json = JsonSerializer.Serialize(dtos);
      File.WriteAllText(path, json);
   }

   public static List<MqttPublishMessage> LoadRetainedMessages(string path)
   {
      if (!File.Exists(path)) return [];

      var json = File.ReadAllText(path);
      var dtos = JsonSerializer.Deserialize<List<RetainedMessageDto>>(json);
      
      var messages = new List<MqttPublishMessage>();
      foreach (var dto in dtos)
      {
         var packet = new PublishPacket
         {
            QualityOfService = (QualityOfServiceType)dto.QualityOfService,
            Retain = dto.Retain,
            TopicUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(dto.Topic)),
            Payload = new ReadOnlySequence<byte>(Convert.FromBase64String(dto.PayloadBase64))
         };
         messages.Add(new MqttPublishMessage(packet));
      }
      return messages;
   }
}
```

### Subscribing to Server Hooks

```csharp
// Load retained messages on startup
mqttServer.Events.OnLoadingRetainedMessages.Add((context, ct) =>
{
   context.LoadedRetainedMessages = ServerPersistenceHelper.LoadRetainedMessages("retained_messages.json");
   return ValueTask.CompletedTask;
});

// Save retained messages whenever they are updated or cleared
mqttServer.Events.OnRetainedMessageChanged.Add((context, ct) =>
{
   ServerPersistenceHelper.SaveRetainedMessages("retained_messages.json", context.StoredRetainedMessages);
   return ValueTask.CompletedTask;
});
```

---

## 3. Client-Side Disconnection Safety & Buffering

When a client loses its connection, calling `PublishAsync` directly returns an error. A production-ready client must buffer outgoing messages and transmit them once the connection is restored.

Depending on the message type, there are two primary buffering strategies:

### 1. Offline Message Queue (FIFO)
For event streams (e.g. log events, transactions, alerts), order and completeness are vital.
- Every message published while offline is added to a FIFO queue (`ConcurrentQueue`).
- When connection is restored, all queued events are flushed in order.

### 2. Last Message Buffer (State Cache)
For state variables (e.g. temperature, machine status, battery level), only the most up-to-date reading is useful.
- Outgoing publishes overwrite previous ones in a key-value map (`ConcurrentDictionary<string, Message>`).
- When connection is restored, only the latest state of each topic is flushed, preventing network flooding and processing stale data.

### Implementing a Buffered Client Wrapper

Below is an overview of how to implement a `BufferedMqttClient` wrapper. (A complete, runnable implementation is available in [Program.cs](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Examples/Mqtt/Beskar.Mqtt.Example.DisconnectionSafety/Program.cs)).

```csharp
public class BufferedMqttClient
{
   private readonly IMqttClient _client;
   private readonly ConcurrentQueue<SavedPublishDto> _fifoQueue = new();
   private readonly ConcurrentDictionary<string, SavedPublishDto> _lastMessageBuffer = new();

   public BufferedMqttClient(IMqttClient client)
   {
      _client = client;

      // Hook reconnection to auto-flush buffers
      _client.AddConnectedHandler(async (context, ct) =>
      {
         await FlushAsync();
      });
   }

   public async Task PublishBufferedAsync(string topic, string payload, bool isStateMessage)
   {
      var dto = new SavedPublishDto { Topic = topic, Payload = payload, IsStateMessage = isStateMessage };

      if (_client.IsConnected)
      {
         await SendDtoAsync(dto);
      }
      else
      {
         if (isStateMessage)
         {
            _lastMessageBuffer[topic] = dto; // Overwrite older status updates
         }
         else
         {
            _fifoQueue.Enqueue(dto); // Append to event stream
         }
      }
   }

   private async Task FlushAsync()
   {
      // 1. Flush event FIFO queue
      while (_fifoQueue.TryDequeue(out var dto))
      {
         await SendDtoAsync(dto);
      }

      // 2. Flush latest state variables
      var topics = _lastMessageBuffer.Keys.ToArray();
      foreach (var topic in topics)
      {
         if (_lastMessageBuffer.TryRemove(topic, out var dto))
         {
            await SendDtoAsync(dto);
         }
      }
   }
}
```

> [!TIP]
> Just like retained messages, you can serialize/deserialize your client-side offline queues to JSON. Call `SaveToFile()` in the client's shutdown flow or whenever a message is added offline, and `LoadFromFile()` at application startup. This prevents message loss in case of a crash or power failure.

---

## 4. Complete Code Reference

To see these mechanisms in action, refer to the complete, runnable demonstration in the codebase:
- **Example Location**: [Program.cs](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Examples/Mqtt/Beskar.Mqtt.Example.DisconnectionSafety/Program.cs)
- **Features Tested**:
  - Auto-reloading retained messages on server restart.
  - Server-side offline queuing delivery for persistent sessions.
  - Client-side FIFO queueing vs Last-Message state consolidation.
  - Restoring client-side offline queue from JSON.
