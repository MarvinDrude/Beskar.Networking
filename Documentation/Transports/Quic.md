# QUIC Transport Guide (`Beskar.Networking.Transports.Quic`)

The **QUIC Transport** provides UDP-based stream multiplexing, mandatory TLS 1.3 encryption, and resistance to connection drops during IP migration (such as switching between Wi-Fi and mobile data).

---

## 1. Quick Start

```csharp
using System.Net;
using Beskar.Networking.Transports.Quic;

var endPoint = new IPEndPoint(IPAddress.Loopback, 8002);

var options = new QuicTransportOptions
{
   AlpnProtocol = "beskar-quic",
   MaxInboundBidirectionalStreams = 100,
   IdleTimeout = TimeSpan.FromSeconds(10)
};

// Server Listener
await using var listener = new QuicNetworkListener(endPoint, options);
await listener.BindAsync();

// Client Connection
await using var client = new QuicNetworkClient(options);
var sessionResult = await client.ConnectAsync(endPoint);
```

---

## 2. Stream & Connection Tuning

### ALPN Protocol (`AlpnProtocol`)
- **Default**: `"beskar-quic"`
- **Requirement**: Application-Layer Protocol Negotiation (ALPN) is mandatory in QUIC. Both client and server **MUST** specify identical ALPN strings, or the TLS 1.3 handshake will be immediately rejected.

### Stream Limits & Idle Timeouts
- **`MaxInboundBidirectionalStreams`**: Defaults to `100`. Limits concurrent streams opened by a peer on a single QUIC connection to prevent resource exhaustion.
- **`IdleTimeout`**: Defaults to `5 seconds`. Automatically cleans up inactive QUIC connections.
- **`KeepAliveInterval`**: Optional keep-alive ping to maintain NAT UDP bindings across mobile carrier networks.

---

## 3. Platform Dependencies (MsQuic on Windows vs Linux)

QUIC implementation relies on Microsoft's **MsQuic** (`libmsquic`) library.

### Windows Requirements
- Supported on **Windows 11** and **Windows Server 2022+** out of the box (`msquic.dll`).

### Linux Requirements
- Requires `libmsquic` installed on the host operating system:
  ```bash
  # Ubuntu / Debian
  sudo apt-get install -y libmsquic
  ```
- **UDP Kernel Buffers**: Increase Linux UDP buffer caps to handle high-frequency datagrams without OS packet drops:
  ```bash
  sysctl -w net.core.rmem_max=8388608
  sysctl -w net.core.wmem_max=8388608
  ```

---

## 4. Mandatory Security & Certificates

QUIC requires **TLS 1.3** by design; unencrypted QUIC connections are not permitted by the specification.

```csharp
options.SslServerOptions = new SslServerAuthenticationOptions
{
   ServerCertificate = new X509Certificate2("quic_server.pfx", "password"),
   ApplicationProtocols = new List<SslApplicationProtocol>
   {
      new SslApplicationProtocol("beskar-quic")
   }
};
```

> [!NOTE]
> If `SslServerOptions` is omitted during local development, `Beskar.Networking.Transports.Quic` generates a temporary self-signed developer certificate automatically. Production environments must provide a valid certificate.
