# In-Memory Transport Guide (`Beskar.Networking.Transports.Memory`)

The **In-Memory Transport** provides zero-allocation in-process communication using paired `System.IO.Pipelines` memory channels (`MemoryEndPoint`).

---

## 1. Quick Start

```csharp
using Beskar.Networking.Transports.Memory;

var address = "test-in-memory-pipe";
var endPoint = new MemoryEndPoint(address);

var options = new MemoryTransportOptions
{
   MaxPendingConnections = 1024
};

// Server Listener
await using var listener = new MemoryNetworkListener(endPoint, options);
await listener.BindAsync();

// Client Connection
await using var client = new MemoryNetworkClient(options);
var sessionResult = await client.ConnectAsync(endPoint);
```

---

## 2. Performance & Use Cases

- **Throughput**: Achieves over **8,300,000 packets/sec** with zero memory allocations.
- **No Kernel Context Switches**: Directly passes memory buffers between producer and consumer pipeline queues in-process.

### Primary Use Cases
1. **Unit & Integration Testing**: Test high-level application handlers and protocols (like MQTT or Resilient) without binding real OS network ports or firewalls.
2. **In-Process Module Decoupling**: Connect modular application subsystems via transport-agnostic `INetworkClient` / `INetworkListener` abstractions.
