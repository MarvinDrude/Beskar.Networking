# MQTT Last Will & Testament (LWT)

The **Last Will & Testament (LWT)** feature allows a client to specify a topic and a payload (and optionally delay intervals, QoS, and retain options) when it first connects to the broker. 

If the client disconnects **ungracefully** (due to a network loss, keep-alive timeout, or crash), the broker will automatically publish the specified "Will" message to notify other clients (e.g. system monitors or dashboards) that the device is offline.

If the client disconnects **gracefully** by sending a `DISCONNECT` packet before closing the socket, the broker will discard the Will message.

---

## 1. Configuring Will Options on Connection

When constructing the connection parameters on the client side, configure the `Will` properties on `ConnectOptions`:

```csharp
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;
using System.Buffers;
using System.Text;

var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, 1883),
   ProtocolVersion = MqttProtocolVersion.V50,
   
   // Enable and configure LWT
   HasWill = true,
   WillTopicUtf8Bytes = Encoding.UTF8.GetBytes("device/status/presence"),
   WillPayload = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("offline_unexpected")),
   WillQualityOfService = QualityOfServiceType.AtLeastOnce,
   WillRetain = true // Retain message so new subscribers see the offline status
};

await mqttClient.ConnectAsync(connectOptions);
```

---

## 2. Graceful vs. Ungraceful Disconnection

### Graceful Disconnect
When the client calls `DisconnectAsync`, it sends a `DISCONNECT` packet to the server. The server processes this packet and discards the client's configured Will message:

```csharp
// Graceful: will NOT trigger the Last Will message
await mqttClient.DisconnectAsync(new DisconnectOptions());
```

### Ungraceful Disconnect
If the socket connection is broken abruptly (e.g., calling `DisposeAsync` directly without disconnecting, network failure, or keep-alive check failure), the server realizes the connection was lost without a `DISCONNECT` packet and publishes the Will message to the designated topic:

```csharp
// Ungraceful: will trigger the Last Will message on the broker
await mqttClient.DisposeAsync();
```

---

## 3. MQTT v5 Will Delay Interval

MQTT v5.0 introduces the **Will Delay Interval** (measured in seconds). 
- If configured, when the client disconnects ungracefully, the broker will wait for the specified delay before publishing the Will message.
- If the client successfully reconnects within this delay window, the Will message is **not** published, preventing false-positive disconnect alerts during quick network handoffs.

```csharp
var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, 1883),
   ProtocolVersion = MqttProtocolVersion.V50,
   HasWill = true,
   WillTopicUtf8Bytes = Encoding.UTF8.GetBytes("device/status/presence"),
   WillPayload = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes("offline")),
   
   // Wait 30 seconds before sending the Will. If client reconnects, cancel it.
   WillDelayInterval = 30 
};
```

---

## 4. Example Code Reference

Refer to the complete, runnable LWT demonstration in the codebase:
- **Example Location**: [Program.cs](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Examples/Mqtt/Beskar.Mqtt.Example.LastWill/Program.cs)
- **Features Verified**:
  - Broker setup and monitor client subscription to the presence topic.
  - Graceful disconnect simulation showing that the LWT is discarded.
  - Ungraceful disconnect simulation showing that the LWT is published.
