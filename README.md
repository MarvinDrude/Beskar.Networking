
<h1>
<p align="center">
   <img src="https://github.com/MarvinDrude/Beskar.Networking/blob/master/Resources/banner.png" alt="Logo" width="256" />
   <br />
   Beskar.Networking
</p>
</h1>
<p align="center">
   Fast, <code>.NET</code> native, high-performance, low-allocation networking library.<br/>
   Built for modern <code>.NET</code> using <code>C#</code> includes TCP, WebSockets, QUIC, and MQTT.<br/>
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
- Performance-first approach with **100.000** - **1.200.000** messages per second. (Depending on various factors)
- Many simple or more advanced example projects demonstrating how to use `Beskar.Networking` across various transports.
- Various Unit Tests to verify functionality and performance.
- Active development and steady progress towards a full feature set.
- MQTT V3 and V5 support. (Server and Client) – Over any transport supported by Beskar.
- No external runtime dependencies besides .NET and Beskar.

---

## NuGet packages overview

| Package / Project | Description                                                                                         | NuGet Link |
| :--- |:----------------------------------------------------------------------------------------------------| :--- |
| **Beskar.Mqtt.Server** | High-performance, low-allocation MQTT broker/server support for v3.1.1 and v5.0 over any transport. | *Under development* |
| **Beskar.Mqtt.Client** | Lightweight and efficient MQTT client supporting v3.1.1 and v5.0 over any transport.                | *Under development* |
| **Beskar.Networking.Transports.Tcp** | High-performance, native TCP transport implementation supporting TLS.                               | *Under development* |
| **Beskar.Networking.Transports.Ws** | WebSocket (WS/WSS) transport adapter wrapping custom framed duplex pipelines.                       | *Under development* |
| **Beskar.Networking.Transports.Quic** | Multiplexed and secure QUIC transport implementation built on native .NET libraries.                | *Under development* |

## Examples

We provide both basic and advanced example projects demonstrating how to use `Beskar.Networking` across various transports:

All examples: [**Root Folder**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples)

- [**Simple Ping-Pong Message (Bare-metal)**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Simple/Beskar.Bare.PingPongMessage): Demonstrates low-level server listener binding, client connection, and raw bidirectional message exchange using length-prefixed framing over `System.IO.Pipelines` using TCP.
- [**Advanced Multi-Transport Chat Application**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Advanced): A complete chat application consisting of:
  - [**Common**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Advanced/Beskar.Adv.Chat.Common): Reusable packet serialization and custom packet framing utilities.
  - [**Server**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Advanced/Beskar.Adv.Chat.Server): Integrates a unified `ChatServerBuilder` to listen and accept chat sessions concurrently on TCP (9000), WebSockets (11000), and QUIC (12000).
  - [**Client**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Advanced/Beskar.Adv.Chat.Client): An interactive console application allowing users to connect via their choice of protocol (TCP/WS/QUIC), receive recent chat history, and chat in real-time under a random username.

---

## Documenation

Under development.

## Performance Benchmarks

Engineered from the ground up for maximum throughput and minimum allocation overhead.
> **Computer Specs:**<br/>
> CPU: AMD Ryzen 9 7950X3D,
> Memory: 64 GB DDR5

| Transport | Throughput (msg/s) | Status                   |
| :--- | :--- |:-------------------------|
| **TCP** (No TLS) | **688,000** | Ready                    |
| **WebSockets (WS)** (No TLS) | **630,000** | Ready |
| **QUIC** (TLS) | **260,000** | Ready |
| **MQTT** | *Under Development* | Active Focus / Heavy WIP |

---

## Key Aspects

- **Unified Abstractions**: Write your network layer once and swap between TCP, WebSockets, or QUIC etc. dynamically.
- **Modern .NET 10 Stack**: Heavily leverages `System.IO.Pipelines` (decoupling IO thread queues from application processing), `Span<T>`, `ReadOnlySpan<T>`, and direct memory pooling to achieve zero/near-zero allocations.
- **TLS/SSL Encryption**: Full native SSL/TLS wrapping for TCP and WebSocket transports out of the box.
- **Non-blocking IO Queueing**: Highly-optimized custom IO queues (like `TcpIoQueueRegistry` / `TcpIoQueue`) to handle asynchronous reading and writing concurrently.
- **Full MQTT**:
- **Other Features**: Additional features and capabilities that make this library unique and valuable.

---
