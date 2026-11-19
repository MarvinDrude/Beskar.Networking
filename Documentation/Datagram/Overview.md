# `Beskar.Networking.Datagram` Guide

`Beskar.Networking.Datagram` is a standalone, low-allocation UDP datagram and multi-channel reliability engine designed for real-time applications
(telemetry, simulations, control loops, and multiplayer games). It intentionally lives outside `Beskar.Networking.Abstractions`
(`INetworkClient` / `INetworkListener` / `INetworkSession` / `INetworkStream`) so it operates directly on 1:1 datagrams without byte-stream `IDuplexPipe` framing.

---

## 1. Key Capabilities

- **5 Channel Delivery Modes (`0..63`)** multiplexed into shared MTU-safe datagrams (`MaxDatagramSize`, default `1200` bytes):
  - `UnreliableUnordered`: Fire-and-forget, zero sequence overhead.
  - `UnreliableSequenced`: Drops stale out-of-order arrivals; zero retransmission.
  - `ReliableUnordered`: Guaranteed delivery without head-of-line ordering stalls, with sliding-bitset deduplication.
  - `ReliableOrdered`: Guaranteed strict FIFO delivery per channel.
  - `ReliableSequenced`: Guaranteed convergence to the latest queued state; superseded unacked updates are dropped.
- **Transport-Level Piggybacked ACKs:** Each physical UDP datagram carries a monotonic 32-bit `PacketSeq`, `AckSeq`, and a 64-bit `AckBitfield`, plus a 1024-packet anti-replay/duplicate bitmap.
- **Built-In Cryptographic Handshake & AEAD Encryption:**
  - 4-step stateless cookie challenge (`ConnectRequest` padded to prevent UDP amplification).
  - Still DO NEVER use this UDP for any
  - Ephemeral **ECDH P-256 + HKDF-SHA256** key exchange (plus optional `PreSharedKey`) deriving per-direction `AesGcm` or `ChaCha20Poly1305` keys.
  - Seamless NAT endpoint migration authenticated by `SessionToken` and AEAD tag.
- **Multi-Peer Server Tracking:** `DatagramHost` exposes `Peers`, `TryGetPeer`, `Peer.PeerId`, `Peer.Tag`, `Peer.ConnectMetadata`, `ValidateConnection`, `Broadcast(...)`, and `SendTo(...)`.
- **Dual Consumption API:** Zero-allocation `TryPollEvent(...)` / `PollEvents(...)` for fixed-tick loops, plus `ReadEventsAsync(...)` and `Events` async delegates.
- **Adaptive Congestion & Time Sync:** Jacobson/Karels SRTT/RTTVAR/RTO, fast retransmit, token-bucket byte pacing, and NTP-style `TimeSyncPing`/`Pong` clock-offset estimation.
- **Single-Socket NAT Hole Punching:** Built-in `DatagramRendezvousServer`, `DiscoverPublicEndPointAsync`, `PunchAsync`, and `PunchAndConnectAsync` sharing the host's bound socket.

---

## 2. Quick Start

```csharp
var options = new DatagramOptions()
   .AddChannel(0, DatagramDeliveryMode.ReliableOrdered, priority: 255)
   .AddChannel(1, DatagramDeliveryMode.UnreliableSequenced, priority: 150)
   .AddChannel(2, DatagramDeliveryMode.ReliableUnordered, priority: 200);

await using var server = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 9000), options);
server.Events.ValidateConnection = ctx =>
{
   ctx.PeerTag = Encoding.UTF8.GetString(ctx.ConnectMetadata.Span);
   return ValueTask.CompletedTask;
};
server.Start();

await using var client = new DatagramHost(options);
var serverPeer = await client.ConnectAsync(server.LocalEndPoint!, "player-1"u8.ToArray());
serverPeer.TrySend(0, "ready"u8);
```
