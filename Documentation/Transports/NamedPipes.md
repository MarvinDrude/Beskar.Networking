# Named Pipes Transport Guide (`Beskar.Networking.Transports.NamedPipes`)

The **Named Pipes Transport** provides high-throughput local Inter-Process Communication (IPC) using OS Named Pipes (`NamedPipeEndPoint`).

---

## 1. Quick Start

```csharp
using Beskar.Networking.Transports.NamedPipes;

var pipeName = "beskar-local-ipc";
var endPoint = new NamedPipeEndPoint(pipeName);

var options = new NamedPipeTransportOptions
{
   InputBufferSize = 64 * 1024,
   OutputBufferSize = 64 * 1024
};

// Server Listener
await using var listener = new NamedPipeNetworkListener(endPoint, options);
await listener.BindAsync();

// Client Connection
var clientOptions = new NamedPipeTransportOptions();
clientOptions.StreamOptions.IoQueueCount = 1;
await using var client = new NamedPipeNetworkClient(clientOptions);
var sessionResult = await client.ConnectAsync(endPoint);
```

---

## 2. Pipe Buffer Tuning

- **`InputBufferSize` & `OutputBufferSize`**: Default `64 KB` (`65,536` bytes) per pipe instance.
- **`MaxConcurrentHandshakes`**: Default `512` concurrent client connections.
- **`MaxPendingConnections`**: Default `1024`.

---

## 3. Platform Differences (Windows vs Linux)

### Windows (Native Named Pipes)
- Named Pipes use the Windows kernel Named Pipe file system (`\\.\pipe\PipeName`).
- Provides maximum throughput for local Windows services, desktop apps, and IIS worker processes.

### Linux (.NET Emulation)
- On Linux, .NET emulates Named Pipes using Unix Domain Sockets under the hood, writing socket files to local temporary directories.

---

## 4. Security & Access Control

On Windows, Named Pipes security relies on Windows process tokens and Access Control Lists (ACLs). Access is restricted to processes running on the local machine under authorized Windows security identifiers (SIDs).
