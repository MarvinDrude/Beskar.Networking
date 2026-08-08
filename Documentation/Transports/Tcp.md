# TCP Transport Guide (`Beskar.Networking.Transports.Tcp`)

The **TCP Transport** is the core stream-based networking transport in `Beskar.Networking`.
It delivers ultra-high throughput and sub-millisecond latency for client-server architectures over Local Area
Networks (LAN) and Wide Area Networks (WAN).

---

## 1. Quick Start

```csharp
using System.Net;
using Beskar.Networking.Transports.Tcp;

var endPoint = new IPEndPoint(IPAddress.Loopback, 9000);

// Server Listener
var options = new TcpTransportOptions
{
   NoDelay = true,
   SendBufferSize = 512 * 1024,
   ReceiveBufferSize = 512 * 1024
};

await using var listener = new TcpNetworkListener(endPoint, options);
await listener.BindAsync();

// Client Connection
var clientOptions = new TcpTransportOptions();
clientOptions.SocketOptions.IoQueueCount = 1; // Client optimization
await using var client = new TcpNetworkClient(clientOptions);
var result = await client.ConnectAsync(endPoint);
```

---

## 2. Socket & Buffer Tuning

### Nagle's Algorithm (`NoDelay`)
- **Default**: `true`
- **Impact**: Disables TCP Nagle's algorithm (`TCP_NODELAY`), sending packets immediately without waiting to coalesce small writes. Crucial for real-time messaging, games, and financial streaming.

### Socket Kernel Buffers (`SendBufferSize` & `ReceiveBufferSize`)
- **Default**: `512 KB` (`524,288` bytes)
- **Tuning**: Set `null` to let the Operating System dynamically manage TCP window scaling (`SO_SNDBUF` / `SO_RCVBUF`). For high-bandwidth 10GbE+ networks, increase buffers to 2MB - 8MB.

### Pipeline Memory & IO Queues (`IoQueueCount`)
- **Server Applications**: Default `IoQueueCount` uses `Math.Min(Environment.ProcessorCount, 12)` non-blocking queue workers to distribute IO load evenly across CPU cores.
- **Client Applications**: Always set `options.SocketOptions.IoQueueCount = 1` and `options.StreamOptions.IoQueueCount = 1` on client applications to prevent allocating unused thread queues and pinned memory pools.

---

## 3. Platform & Kernel Differences (Windows vs Linux)

### Linux Performance Tuning
- **Backlog**: The `Backlog` parameter controls the `listen()` socket queue depth. Ensure the Linux kernel `net.core.somaxconn` setting is adjusted accordingly:
  ```bash
  sysctl -w net.core.somaxconn=4096
  ```
- **Socket Buffer Caps**: Ensure `net.core.rmem_max` and `net.core.wmem_max` allow 512 KB+ socket allocations.

### Windows (IOCP)
- Uses I/O Completion Ports (IOCP) for asynchronous completion notifications. Ensure `MaxConcurrentHandshakes` (default `512`) matches high-volume connection spikes.

---

## 4. Security & SSL/TLS Configuration

TLS 1.2 and TLS 1.3 encryption are natively integrated via `SslServerAuthenticationOptions` and `SslClientAuthenticationOptions`.

```csharp
var options = new TcpTransportOptions
{
   UseSsl = true,
   SslHandshakeTimeout = TimeSpan.FromSeconds(10),
   SslServerOptions = new SslServerAuthenticationOptions
   {
      ServerCertificate = new X509Certificate2("server.pfx", "password"),
      ClientCertificateRequired = false,
      EnabledSslProtocols = SslProtocols.Tls12 | SslProtocols.Tls13
   }
};
```

> [!SECURITY]
> **Slowloris Defense**: `SslHandshakeTimeout` (default: 10 seconds) automatically terminates unauthenticated
> clients that open TCP connections without completing the TLS handshake, mitigating Denial-of-Service attacks.
