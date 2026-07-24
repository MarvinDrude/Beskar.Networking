
<h1>
<p align="center">
   <img src="https://github.com/MarvinDrude/Beskar.Networking/blob/master/Resources/banner.png" alt="Logo" width="256" />
   <br />
   Beskar.Networking
</p>
</h1>
<p align="center">
   Fast, <code>.NET</code> native, high-performance, low-allocation networking library.<br/>
   Built for modern <code>C#</code> includes TCP, WebSockets, QUIC, UDP, UDS, Named Pipes, Memory, and MQTT.<br/>
   No external runtime dependencies besides <b>Beskar</b>.<br/><br/>
   <a href="#about">About</a>
   ·
   <a href="#examples">Examples</a>
   ·
   <a href="#documenation">Documentation</a>
   ·
   <a href="#performance-benchmarks">Performance</a>
   ·
   <a href="#key-aspects">Key Aspects</a>
</p>
<br/>

---
<br/>

![Code Poetry](https://img.shields.io/badge/code-is_poetry-orange)
![Issues](https://img.shields.io/github/issues/MarvinDrude/Beskar.Networking)
![Repo Size](https://img.shields.io/github/repo-size/MarvinDrude/Beskar.Networking.svg)
[![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)](https://raw.githubusercontent.com/MarvinDrude/Beskar.Networking/master/LICENSE.md)

## About

Main reasons for why you should consider using `Beskar.Networking` for your next **Networking** or **MQTT** use case:

- Made with passion and love for the craft.
- Built for modern `.NET` using `C#` and designed to be highly performant, low-allocation, and easy to use.
- Purposefully designed to be lightweight and flexible.
- Performance-first approach with **100,000** - **10,800,000** messages/packets per second. (Depending on various factors)
- Many simple or more advanced example projects demonstrating how to use `Beskar.Networking` across various transports.
- Various Unit Tests to verify functionality and performance.
- Active development and steady progress towards a full feature set.
- MQTT V3 and V5 support. (Server and Client) – Over any transport supported by Beskar.
- No external runtime dependencies besides .NET and Beskar.

---

## NuGet packages overview

| Package / Project | Description                                                                                         | NuGet Link |
| :--- |:----------------------------------------------------------------------------------------------------| :--- |
| **Beskar.Mqtt.Server** | High-performance, low-allocation MQTT broker/server support for v3.1.1 and v5.0 over any transport. | [![NuGet Version](https://img.shields.io/nuget/v/Beskar.Mqtt.Server.svg)](https://www.nuget.org/packages/Beskar.Mqtt.Server/) |
| **Beskar.Mqtt.Client** | Lightweight and efficient MQTT client supporting v3.1.1 and v5.0 over any transport.                | [![NuGet Version](https://img.shields.io/nuget/v/Beskar.Mqtt.Client.svg)](https://www.nuget.org/packages/Beskar.Mqtt.Client/) |
| **Beskar.Networking.Resilient.Server** | High-performance, event-driven resilient server wrapper supporting keep-alives, handshakes, and custom framing over any transport. | [![NuGet Version](https://img.shields.io/nuget/v/Beskar.Networking.Resilient.Server.svg)](https://www.nuget.org/packages/Beskar.Networking.Resilient.Server/) |
| **Beskar.Networking.Resilient.Client** | High-performance resilient client wrapper supporting automatic reconnection, keep-alives, handshakes, and custom framing. | [![NuGet Version](https://img.shields.io/nuget/v/Beskar.Networking.Resilient.Client.svg)](https://www.nuget.org/packages/Beskar.Networking.Resilient.Client/) |
| **Beskar.Networking.Transports.Tcp** | High-performance, native TCP transport implementation supporting TLS.                               | [![NuGet Version](https://img.shields.io/nuget/v/Beskar.Networking.Transports.Tcp.svg)](https://www.nuget.org/packages/Beskar.Networking.Transports.Tcp/) |
| **Beskar.Networking.Transports.Ws** | WebSocket (WS/WSS) transport adapter wrapping custom framed duplex pipelines.                       | [![NuGet Version](https://img.shields.io/nuget/v/Beskar.Networking.Transports.Ws.svg)](https://www.nuget.org/packages/Beskar.Networking.Transports.Ws/) |
| **Beskar.Networking.Transports.Quic** | Multiplexed and secure QUIC transport implementation built on native .NET libraries.                | [![NuGet Version](https://img.shields.io/nuget/v/Beskar.Networking.Transports.Quic.svg)](https://www.nuget.org/packages/Beskar.Networking.Transports.Quic/) |
| **Beskar.Networking.Transports.Udp** | High-performance, virtualized session-multiplexed UDP transport implementation.                     | [![NuGet Version](https://img.shields.io/nuget/v/Beskar.Networking.Transports.Udp.svg)](https://www.nuget.org/packages/Beskar.Networking.Transports.Udp/) |
| **Beskar.Networking.Transports.Uds** | Native Unix Domain Sockets (UDS) local transport implementation.                                    | [![NuGet Version](https://img.shields.io/nuget/v/Beskar.Networking.Transports.Uds.svg)](https://www.nuget.org/packages/Beskar.Networking.Transports.Uds/) |
| **Beskar.Networking.Transports.NamedPipes** | Native local Inter-Process Communication (IPC) Named Pipes transport implementation.                 | [![NuGet Version](https://img.shields.io/nuget/v/Beskar.Networking.Transports.NamedPipes.svg)](https://www.nuget.org/packages/Beskar.Networking.Transports.NamedPipes/) |
| **Beskar.Networking.Transports.Memory** | Fast, zero-allocation local in-memory transport implementation for tests and local IPC.            | [![NuGet Version](https://img.shields.io/nuget/v/Beskar.Networking.Transports.Memory.svg)](https://www.nuget.org/packages/Beskar.Networking.Transports.Memory/) |

---

## API Overview

`Beskar.Networking` offers different levels of abstraction depending on your application needs:

### Low-Level Transports

The core library features protocol-agnostic, low-level interfaces:
* `INetworkListener` - The Server listener that accepts incoming connections and manages sessions.
* `INetworkClient` - The client that initiates connections and manages sessions.
* `INetworkSession` - Represents a single connection between a client and server.
* `INetworkStream` - Provides transport-agnostic handling (supporting TCP, WebSockets, QUIC, UDP, UDS, Named Pipes, or Memory).

These interfaces provide transport-agnostic handling (supporting TCP, WebSockets, QUIC, UDP, UDS, Named Pipes, or Memory).
Because they are low-level, you are responsible for managing execution tasks yourself, such as
starting your own accept loops, launching read/write tasks, and supervising session lifecycles.

> [!IMPORTANT]
> **Client Transport Optimization (Low-Level APIs)**
> When instantiating `TcpNetworkClient`, `WsNetworkClient`, `QuicNetworkClient`, `UdsNetworkClient`, or `NamedPipeNetworkClient` directly with custom options (`TcpTransportOptions`, `WsTransportOptions`, `QuicTransportOptions`, `UdsTransportOptions`, `NamedPipeTransportOptions`), the default `IoQueueCount` is optimized for servers and defaults to `Math.Min(Environment.ProcessorCount, 24)`.
>
> For **client-side applications**, you should explicitly configure the option's `IoQueueCount` (e.g. `options.StreamOptions.IoQueueCount = 1` and `options.SocketOptions.IoQueueCount = 1`) to **`1`**. Since a client only manages a single connection, this prevents allocating unnecessary idle memory pools and thread queues, drastically reducing the unmanaged and pinned memory footprint.

For more details, see the [Basics Documentation](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Documentation/Basics).

### High-Level Managed APIs

For protocols like MQTT, or connection-resilient networking, `Beskar.Networking` provides fully managed implementations:
* **Fully Managed Broker & Client (MQTT)**: Pre-built wrappers that manage the connection, session, and stream states automatically.
* **Resilient Client & Server**: Connection-resilient managed wrappers (`ResilientClient<TFrame>` and `ResilientServer<TFrame>`) that handle transient disconnect supervision, automatic pings/pongs (keep-alive), protocol control handshakes, custom challenge-response authentication, and pluggable framing protocol generation.
* **Event-Driven**: Fully event-driven design with convenient events to react to incoming messages, client connections, and status updates, eliminating the need to write custom accept loops or pipeline plumbing.

For more details, see the [Resilient Documentation](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Documentation/Resilient/Overview.md) and [Mqtt Documentation](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Documentation/Mqtt/PubSub.md).

---

## Examples

We provide both basic and advanced example projects demonstrating how to use `Beskar.Networking` across various transports:

All examples: [**Root Folder**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples)

- [**Simple Ping-Pong Message (Bare-metal)**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Simple/Beskar.Bare.PingPongMessage): Demonstrates low-level server listener binding, client connection, and raw bidirectional message exchange using length-prefixed framing over `System.IO.Pipelines` using TCP.
- [**Advanced Multi-Transport Chat Application**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Advanced): A complete chat application consisting of:
  - [**Common**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Advanced/Beskar.Adv.Chat.Common): Reusable packet serialization and custom packet framing utilities.
  - [**Server**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Advanced/Beskar.Adv.Chat.Server): Integrates a unified `ChatServerBuilder` to listen and accept chat sessions concurrently on TCP (9000), WebSockets (11000), and QUIC (12000).
  - [**Client**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Advanced/Beskar.Adv.Chat.Client): An interactive console application allowing users to connect via their choice of protocol (TCP/WS/QUIC), receive recent chat history, and chat in real-time under a random username.
- [**MQTT Managed Examples**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Mqtt): A set of projects demonstrating the high-level managed MQTT APIs:
  - [**Simple Publish-Subscribe**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Mqtt/Beskar.Mqtt.Example.SimplePubSub): A complete pub-sub flow showing how to spin up an MQTT broker, connect a sensor client (publisher) and dashboard client (subscriber) over TCP using MQTT v5.0.
  - [**Generated Pub-Sub (Source Generator)**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Mqtt/Beskar.Mqtt.Example.GeneratedPubSub): Showcases how to use the high-performance MQTT Topic Source Generator to define compile-time validated topics and perform zero-allocation publishing using the generated `byte[]` format overloads.
  - [**Quality of Service (QoS) Levels**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Mqtt/Beskar.Mqtt.Example.QosLevels): Demonstrates publishing and subscribing messages with different QoS levels (QoS 0: At Most Once, QoS 1: At Least Once, QoS 2: Exactly Once) using wildcard topic filters.
  - [**Authentication (v3 & v5 Challenges)**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Mqtt/Beskar.Mqtt.Example.Authentication): Highlights both simple username/password validation for MQTT v3.1.1 and advanced challenge-response authentication for MQTT v5.0.
  - [**Client Reconnection & Event Handling**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Mqtt/Beskar.Mqtt.Example.Reconnection): Shows how to register connection status events and implement custom auto-reconnection loops when connections are unexpectedly lost.
- [**Resilient Managed Examples**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Resilient): A set of projects demonstrating the high-level connection-resilient server and client wrappers:
  - [**Resilient Chat Application**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Resilient): A complete chat application using the resilient wrappers with default `BeskarPacket` framing and automated JSON serialization:
    - [**Common**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Resilient/Beskar.Resilient.Chat.Common): Contains shared message payloads and a JSON `ChatSerializer` implementing `IResilientSerializer`.
    - [**Server**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Resilient/Beskar.Resilient.Chat.Server): Integrates `ResilientServer<BeskarPacket>` to accept, track, and broadcast chat messages over TCP port 9000.
    - [**Client**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Resilient/Beskar.Resilient.Chat.Client): Interactive chat client using `ResilientClient<BeskarPacket>` that reconnects and re-authenticates/joins automatically when connection is lost.
  - [**Simple Ping-Pong**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Resilient/Beskar.Resilient.PingPong): Demonstrates basic ping-pong message exchange using `ResilientServer` and `ResilientClient` with default framing over TCP port 9001.
  - [**Authentication Challenge-Response**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Resilient/Beskar.Resilient.Authentication): Highlights pre-handshake HMAC-SHA256 signature challenge-response authentication, showing both success and server-denial test cases on TCP port 9002.

---

## Documenation

You can find detailed documentation for `Beskar.Networking` [here](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Documentation).

Or you can find examples directly [here](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples).

## Performance Benchmarks

Engineered from the ground up for maximum throughput and minimum allocation overhead.
> [!NOTE]\
> **Computer Specs:**<br/>
> CPU: AMD Ryzen 9 7950X3D,
> Memory: 64 GB DDR5
>
> **Method**: Payload does not include protocol, Local Loopback Benchmark<br />
> **Note:** Benchmarks are subject to change as the library evolves.

| Transport / Protocol | Clients | Payload Size | Throughput | Bandwidth |
| :--- | :---: | :---: | :--- | :--- |
| **TCP** (No TLS) | 20 | 128 bytes | 10,805,651 packets/s | 1,319.05 MB/s |
| **TCP** (No TLS) | 20 | 512 bytes | 7,355,303 packets/s | 3,591.46 MB/s |
| **WebSockets (WS)** (No TLS) | 20 | 128 bytes | 10,784,817 packets/s | 1,316.51 MB/s |
| **WebSockets (WS)** (No TLS) | 20 | 512 bytes | 6,968,596 packets/s | 3,402.63 MB/s |
| **QUIC** | 20 | 128 bytes | 3,503,372 packets/s | 427.66 MB/s |
| **QUIC** | 20 | 512 bytes | 954,711 packets/s | 466.17 MB/s |
| **MQTT** (TCP - No TLS) | 20 | 128 bytes | 918,794.17 msg/s | 112.16 MB/s |
| **MQTT** (TCP - No TLS) | 20 | 512 bytes | 674,086.45 msg/s | 329.14 MB/s |
| **UDP** | 20 | 128 bytes | 3,480,517 packets/s | 424.87 MB/s |
| **UDP** | 20 | 512 bytes | 801,432 packets/s | 391.32 MB/s |
| **Unix Domain Sockets (UDS)** | 20 | 128 bytes | 11,993,784 packets/s | 1,464.08 MB/s |
| **Unix Domain Sockets (UDS)** | 20 | 512 bytes | 9,487,101 packets/s | 4,632.37 MB/s |
| **Named Pipes** | 20 | 128 bytes | 12,777,198 packets/s | 1,559.72 MB/s |
| **Named Pipes** | 20 | 512 bytes | 5,739,487 packets/s | 2,802.48 MB/s |
| **Memory** | 20 | 128 bytes | 8,388,522 packets/s | 1,023.99 MB/s |
| **Memory** | 20 | 512 bytes | 7,728,970 packets/s | 3,773.91 MB/s |

---

## Key Aspects

`Beskar.Networking` is a high-performance, low-allocation networking library built for modern **.NET 10** using C#.
It provides a unified, pipe-based interface abstraction (`INetworkListener`, `INetworkClient`, `INetworkSession`, `INetworkStream`)
for building extremely fast network applications across multiple transport protocols.

- **Unified Abstractions**: Write your network layer once and swap between TCP, WebSockets, QUIC, UDP, UDS, Named Pipes, or Memory dynamically.
- **Modern .NET 10 Stack**: Heavily leverages `System.IO.Pipelines` (decoupling IO thread queues from application processing), `Span<T>`, `ReadOnlySpan<T>`, and direct memory pooling to achieve zero/near-zero allocations.
- **TLS/SSL Encryption**: Full native SSL/TLS wrapping for TCP and WebSocket transports out of the box.
- **Non-blocking IO Queueing**: Highly-optimized custom IO queues (like `TcpIoQueueRegistry` / `TcpIoQueue`) to handle asynchronous reading and writing concurrently.
- **Full MQTT**:
- **Other Features**: Additional features and capabilities that make this library unique and valuable.

---
