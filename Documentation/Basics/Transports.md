# Transports & Transport-Agnostic Design

Beskar.Networking separates transport-specific details (like sockets, framing, and protocols) from your application logic. This allows you to write your protocol handlers once and run them on top of TCP, WebSockets, or QUIC interchangeably.

---

## 1. Instantiating Specific Transports

Each transport package contains its own concrete implementation of `INetworkListener` and `INetworkClient`.

### TCP Transport (`Beskar.Networking.Transports.Tcp`)
TCP is the standard, low-overhead transport for stream-based socket connections.

```csharp
using System.Net;
using Beskar.Networking.Transports.Tcp;

// Client Connection
var client = new TcpNetworkClient(new TcpTransportOptions());
var clientSessionResult = await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, 8000));

// Server Listener
var listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Any, 8000), new TcpTransportOptions());
```

### WebSocket Transport (`Beskar.Networking.Transports.Ws`)
WebSockets allow your server to connect to web browsers, running over HTTP/HTTPS ports.

```csharp
using System.Net;
using Beskar.Networking.Transports.Ws;

// Client Connection
var client = new WsNetworkClient(new WsTransportOptions());
var clientSessionResult = await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, 8001));

// Server Listener
var listener = new WsNetworkListener(new IPEndPoint(IPAddress.Any, 8001), new WsTransportOptions());
```

### QUIC Transport (`Beskar.Networking.Transports.Quic`)
QUIC runs over UDP, providing built-in encryption (TLS 1.3), stream multiplexing, and resistance
to connection drops during IP migration (ideal for mobile networks).

```csharp
using System.Net;
using Beskar.Networking.Transports.Quic;

// Client Connection
var client = new QuicNetworkClient(new QuicTransportOptions());
var clientSessionResult = await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, 8002));

// Server Listener
var listener = new QuicNetworkListener(new IPEndPoint(IPAddress.Any, 8002), new QuicTransportOptions());
```

---

## 2. Interface Agnosticism (Writing General Logic)

To keep your application code reusable, you should never write code that depends directly on concrete
classes like `TcpNetworkSession` or `QuicNetworkStream`. Instead, write your handlers against the base interfaces:

```csharp
using System.Threading.Tasks;
using Beskar.Networking.Abstractions.Interfaces;

public class ApplicationProtocolHandler
{
   public async Task HandleIncomingSessionAsync(INetworkSession session)
   {
      Console.WriteLine($"[Server] New session {session.Id} connected via {session.Transport}");

      // Accept a stream from the peer (works for TCP, WebSockets, or QUIC)
      var streamResult = await session.AcceptStreamAsync();
      if (streamResult.Failed) return;

      var stream = streamResult.Success;
      await ProcessDataAsync(stream);
   }

   private async Task ProcessDataAsync(INetworkStream stream)
   {
      var reader = stream.Transport.Input;

      // Handle reading byte packets here...
   }
}
```

---

## 3. The Builder Pattern for Multi-Transport Servers

Often, a server application needs to support multiple transport protocols concurrently
(e.g. accepting IoT devices over standard TCP, and web client connections over WebSockets).

Beskar implements this by storing a collection of generic `INetworkListener` interfaces in
a **Builder** and running the same application logic on top of all of them.

### Defining a Multi-Transport Builder

Below is a demonstration of how this builder is configured (similar to `MqttServerBuilder`):

```csharp
using System.Collections.Generic;
using System.Net;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;
using Beskar.Networking.Transports.Quic;

public sealed class ApplicationServerBuilder
{
   private readonly List<INetworkListener> _listeners = new();

   public ApplicationServerBuilder UseTcp(int port)
   {
      _listeners.Add(new TcpNetworkListener(new IPEndPoint(IPAddress.Any, port), new TcpTransportOptions()));
      return this;
   }

   public ApplicationServerBuilder UseWs(int port)
   {
      _listeners.Add(new WsNetworkListener(new IPEndPoint(IPAddress.Any, port), new WsTransportOptions()));
      return this;
   }

   public ApplicationServerBuilder UseQuic(int port)
   {
      _listeners.Add(new QuicNetworkListener(new IPEndPoint(IPAddress.Any, port), new QuicTransportOptions()));
      return this;
   }

   public ApplicationServer Build()
   {
      return new ApplicationServer(_listeners);
   }
}
```

### Running the Server Agnostically

The server takes the list of `INetworkListener` interfaces and spins up an accept loop task
for each in parallel, processing incoming connections using the exact same handler:

```csharp
using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using Beskar.Networking.Abstractions.Interfaces;

public sealed class ApplicationServer
{
   private readonly List<INetworkListener> _listeners;
   private readonly CancellationTokenSource _cts = new();

   public ApplicationServer(List<INetworkListener> listeners)
   {
      _listeners = listeners;
   }

   public async Task StartAsync()
   {
      foreach (var listener in _listeners)
      {
         // 1. Bind the listener
         await listener.BindAsync(_cts.Token);
         Console.WriteLine($"Listening on {listener.LocalAddress} via {listener.Transport}");

         // 2. Spawn a background accept loop for this listener
         _ = Task.Run(() => AcceptLoopAsync(listener, _cts.Token));
      }
   }

   private async Task AcceptLoopAsync(INetworkListener listener, CancellationToken token)
   {
      while (!token.IsCancellationRequested)
      {
         try
         {
            // 3. Accept any connection on this transport
            var sessionResult = await listener.AcceptSessionAsync(token);
            if (sessionResult.Failed) continue;

            // 4. Route connection to the identical application logic
            _ = HandleClientSessionAsync(sessionResult.Success);
         }
         catch (Exception)
         {
            // Log or handle listener errors
         }
      }
   }

   private async Task HandleClientSessionAsync(INetworkSession session)
   {
      // Universal logic for all clients...
   }
}
```
