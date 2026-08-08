# UDP Transport Guide (`Beskar.Networking.Transports.Udp`)

The **UDP Transport** delivers high-frequency datagram messaging over virtualized session channels.
It provides ultra-fast data transmission without TCP handshake latency.

---

## 1. Quick Start

```csharp
using System.Net;
using Beskar.Networking.Transports.Udp;

var endPoint = new IPEndPoint(IPAddress.Loopback, 8003);

var options = new UdpTransportOptions
{
   MaxPacketSize = 1400,
   SendBufferSize = 8 * 1024 * 1024,
   ReceiveBufferSize = 8 * 1024 * 1024,
   ClientSessionIdleTimeout = TimeSpan.FromSeconds(30)
};

// Server Listener
await using var listener = new UdpNetworkListener(endPoint, options);
await listener.BindAsync();

// Client Connection
await using var client = new UdpNetworkClient(options);
var sessionResult = await client.ConnectAsync(endPoint);
```

---

## 2. Datagram & Buffer Tuning

### Maximum Packet Size (`MaxPacketSize`)
- **Default**: `1,400 bytes`
- **Tuning**: Configured below the standard Ethernet MTU (`1500 bytes`) to prevent IP layer packet fragmentation and packet loss over WAN routes.

### Socket Kernel Buffers (`SendBufferSize` & `ReceiveBufferSize`)
- **Default**: `8 MB` (`8,388,608` bytes)
- **Impact**: UDP has no TCP flow control window. Large socket buffers are required to absorb sudden traffic bursts without kernel-level datagram drops.

### Pipeline Flow Control Thresholds
- **`IncomingPipePauseThreshold`**: `1 MB`
- **`IncomingPipeResumeThreshold`**: `512 KB`
- **`ClientSessionIdleTimeout`**: `30 seconds` (automatically disposes virtual sessions after inactivity).

---

## 3. Platform & Kernel Considerations

### Linux Kernel Tuning
For maximum UDP packet rates (e.g. 1,000,000+ packets/sec), increase system UDP memory caps:

```bash
sysctl -w net.ipv4.udp_mem="4096 87380 16777216"
sysctl -w net.core.rmem_default=8388608
sysctl -w net.core.wmem_default=8388608
```

### Windows Socket Handling
On Windows, `SIO_UDP_CONNRESET` socket control is handled internally to prevent `WSAECONNRESET` exceptions from interrupting socket receive loops when remote endpoints close unexpectedly.

---

## 4. Security & Resiliency Recommendations

- **Connectionless Nature**: UDP does not provide cryptographic handshakes by default. Use `Beskar.Networking.Resilient` wrappers for HMAC challenge-response authentication, sequence numbers, and ping-pong heartbeats over UDP.
