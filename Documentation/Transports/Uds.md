# Unix Domain Sockets (UDS) Transport Guide (`Beskar.Networking.Transports.Uds`)

The **Unix Domain Sockets (UDS) Transport** provides local Inter-Process Communication (IPC) using file-system
socket descriptors (`UnixDomainSocketEndPoint`). It bypasses the network stack, providing ultra-low latency and
zero network packet overhead.

---

## 1. Quick Start

```csharp
using System.Net.Sockets;
using Beskar.Networking.Transports.Uds;

var socketPath = "/tmp/beskar-app.sock";
var endPoint = new UnixDomainSocketEndPoint(socketPath);

var options = new UdsTransportOptions
{
   SendBufferSize = 512 * 1024,
   ReceiveBufferSize = 512 * 1024
};

// Server Listener
await using var listener = new UdsNetworkListener(endPoint, options);
await listener.BindAsync();

// Client Connection
var clientOptions = new UdsTransportOptions();
clientOptions.SocketOptions.IoQueueCount = 1;
await using var client = new UdsNetworkClient(clientOptions);
var sessionResult = await client.ConnectAsync(endPoint);
```

---

## 2. Socket & Buffer Tuning

- **`SendBufferSize` & `ReceiveBufferSize`**: Default `512 KB` (`524,288` bytes).
- **`Backlog`**: Default `1024`.
- **`IoQueueCount`**: For client applications, set `options.SocketOptions.IoQueueCount = 1` to reduce thread & queue overhead.

---

## 3. Platform Compatibility (Linux vs Windows)

### Linux (Primary Target)
- Standard socket path conventions: `/tmp/*.sock` or `/var/run/*.sock`.
- Upon server shutdown, UDS listeners clean up the socket file descriptor automatically.

### Windows Support
- Supported on **Windows 10 (Build 17063+)** and **Windows Server 2019+**.
- File path format on Windows uses standard file system paths (e.g. `C:\Temp\app.sock`).

---

## 4. Security & Permissions

UDS security is enforced by the **Operating System File System permissions**:

```bash
# Restrict socket file access on Linux
chmod 0660 /var/run/beskar-app.sock
chown appuser:appgroup /var/run/beskar-app.sock
```

> [!SECURITY]
> Only local system users with read/write access to the socket file can establish connections.
