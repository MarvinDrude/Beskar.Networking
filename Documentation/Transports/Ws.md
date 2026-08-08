# WebSocket Transport Guide (`Beskar.Networking.Transports.Ws`)

The **WebSocket Transport** wraps custom duplex pipelines in standard RFC 6455 WebSocket frames (`WS` / `WSS`).
It enables seamless communication with web browsers, proxies, and firewall-restricted environments over HTTP/HTTPS ports.

---

## 1. Quick Start

```csharp
using System.Net;
using Beskar.Networking.Transports.Ws;

var endPoint = new IPEndPoint(IPAddress.Loopback, 8001);

// Server Listener
var options = new WsTransportOptions
{
   Path = "/ws",
   AllowedOrigins = new[] { "https://example.com" },
   KeepAliveInterval = TimeSpan.FromSeconds(30)
};

await using var listener = new WsNetworkListener(endPoint, options);
await listener.BindAsync();

// Client Connection
var clientOptions = new WsTransportOptions { Path = "/ws" };
clientOptions.TcpOptions.SocketOptions.IoQueueCount = 1;
await using var client = new WsNetworkClient(clientOptions);
var sessionResult = await client.ConnectAsync(endPoint);
```

---

## 2. Security & CORS Validation

### CORS / Origin Validation (`AllowedOrigins`)
Restrict client connections by specifying allowed origins during the HTTP Upgrade handshake:

```csharp
var options = new WsTransportOptions
{
   AllowedOrigins = new[] { "https://myapp.com", "https://admin.myapp.com" }
};
```

### WSS Encryption (SSL/TLS)
WebSocket TLS encryption (`wss://`) is configured directly through the underlying `TcpOptions`:

```csharp
options.TcpOptions.UseSsl = true;
options.TcpOptions.SslServerOptions = new SslServerAuthenticationOptions
{
   ServerCertificate = new X509Certificate2("certificate.pfx", "password")
};
```
