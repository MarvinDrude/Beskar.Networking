
<h1>
<p align="center">
   <img src="https://github.com/MarvinDrude/Beskar.Networking/blob/master/Resources/banner.png" alt="Logo" width="128" />
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
   <a href="#">Documentation</a>
   ·
   <a href="#performance">Performance</a>
   ·
   <a href="#key-features">Key features</a>
</p>

---
<br/>

> **Work In Progress (WIP)**
> This library is actively under development. Interfaces, implementations, and APIs are subject
> to change before the first stable release.

## About

`Beskar.Networking` is a high-performance, low-allocation networking library built for modern **.NET 10** using C#.
It provides a unified, pipe-based interface abstraction (`INetworkListener`, `INetworkClient`, `INetworkSession`, `INetworkStream`)
for building extremely fast network applications across multiple transport protocols.

---

## Performance Benchmarks

Engineered from the ground up for maximum throughput and minimum allocation overhead.
Below are benchmark measurements showing messages processed per second (over loopback on current development CPU):

| Transport | Throughput (msg/s) | Status                   |
| :--- | :--- |:-------------------------|
| **TCP** (No TLS) | **688,000** | Ready                    |
| **WebSockets (WS)** (No TLS) | **630,000** | Ready |
| **QUIC** (TLS) | **260,000** | Ready |
| **MQTT** | *Under Development* | Active Focus / Heavy WIP |

---

## Key Features

- **Unified Abstractions**: Write your network layer once and swap between TCP, WebSockets, or QUIC dynamically.
- **Modern .NET 10 Stack**: Heavily leverages `System.IO.Pipelines` (decoupling IO thread queues from application processing), `Span<T>`, `ReadOnlySpan<T>`, and direct memory pooling to achieve zero/near-zero allocations.
- **TLS/SSL Encryption**: Full native SSL/TLS wrapping for TCP and WebSocket transports out of the box.
- **Non-blocking IO Queueing**: Highly-optimized custom IO queues (like `TcpIoQueueRegistry` / `TcpIoQueue`) to handle asynchronous reading and writing concurrently.

---

## Architecture & Core Interfaces

The API is fully decoupled, exposing four core interfaces:

1. **`INetworkListener`**: Responsible for binding to local endpoints and accepting sessions.
2. **`INetworkClient`**: Responsible for establishing outbound sessions.
3. **`INetworkSession`**: Represents a connection session (multiplexed or single-channel).
4. **`INetworkStream`**: High-performance pipeline-based stream containing the `IDuplexPipe` for binary I/O.

---

## 🗺️ Roadmap & Active Work

- [x] High-performance **TCP** client/listener with SSL/TLS support
- [x] High-performance **WebSocket** wrapper utilizing Pipelines
- [x] Zero-allocation **QUIC** transport integration
- [ ] **MQTT** Server & Client protocol implementation (Current focus - heavily WIP)
- [ ] Comprehensive performance profiling & benchmark suites
- [ ] Comprehensive examples and docs
