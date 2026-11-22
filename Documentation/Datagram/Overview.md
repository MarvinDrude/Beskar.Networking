# `Beskar.Networking.Datagram` Guide

`Beskar.Networking.Datagram` is a standalone, low-allocation UDP datagram and multi-channel reliability engine designed for real-time applications
(telemetry, simulations, control loops, and multiplayer games). It intentionally lives outside `Beskar.Networking.Abstractions`
(`INetworkClient` / `INetworkListener` / `INetworkSession` / `INetworkStream`) so it operates directly on 1:1 datagrams without byte-stream `IDuplexPipe` framing.

> [!CAUTION]
> **Do NOT transmit confidential, regulated, financial, credential, or sensitive personal data (PII) over `Beskar.Networking.Datagram` (or raw UDP transports).**
>
> Although `DatagramSecurityMode.Aead` uses standard .NET cryptographic primitives (`ECDiffieHellman` P-256, `HKDF-SHA256`, `AesGcm` / `ChaCha20Poly1305`) to protect real-time traffic against casual sniffing, packet tampering, replay attacks, and UDP amplification, its custom UDP handshake is **not** a formally audited TLS/DTLS PKI stack:
> - Without an out-of-band per-session `PreSharedKey` (e.g., issued by an HTTPS matchmaking/auth backend), ephemeral ECDH does not authenticate peer identity against an active Man-in-the-Middle attacker.
> - NAT rendezvous discovery and `PunchProbe` packets are unencrypted (`CRC32C` + `PunchToken`).
>
> For passwords, tokens, PII, payments, or any confidential traffic, always use **TLS over TCP/WebSockets** (`Beskar.Networking.Transports.Tcp` / `Beskar.Networking.Transports.Ws`) or **QUIC** (`Beskar.Networking.Transports.Quic`).

---

## 1. Key Capabilities

- **5 Channel Delivery Modes (`0..63`)** multiplexed into shared MTU-safe datagrams (`MaxDatagramSize`, default `1200` bytes) - see the full [Delivery & Reliability Modes Guide](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Documentation/Datagram/DeliveryModes.md):
  - `UnreliableUnordered`: Fire-and-forget, zero sequence overhead.
  - `UnreliableSequenced`: Drops stale out-of-order arrivals; zero retransmission.
  - `ReliableUnordered`: Guaranteed delivery without head-of-line ordering stalls, with sliding-bitset deduplication.
  - `ReliableOrdered`: Guaranteed strict FIFO delivery per channel.
  - `ReliableSequenced`: Guaranteed convergence to the latest queued state; superseded unacked updates are dropped.
- **Transport-Level Piggybacked ACKs:** Each physical UDP datagram carries a monotonic 32-bit `PacketSeq`, `AckSeq`, and a 64-bit `AckBitfield`, plus a 1024-packet anti-replay/duplicate bitmap.
- **Built-In Handshake & AEAD Protection (For Real-Time Game / Telemetry Traffic):**
  - 4-step stateless cookie challenge (`ConnectRequest` padded to prevent UDP amplification).
  - Ephemeral **ECDH P-256 + HKDF-SHA256** key exchange (plus optional `PreSharedKey`) deriving per-direction `AesGcm` or `ChaCha20Poly1305` keys.
  - Seamless NAT endpoint migration authenticated by `SessionToken`, AEAD tag, and strictly newer non-replayed `PacketSeq`.
- **Multi-Peer Server Tracking:** `DatagramHost` exposes `Peers`, `TryGetPeer`, `Peer.PeerId`, `Peer.Tag`, `Peer.SetProperty` / `TryGetProperty`, `Peer.ConnectMetadata`, `ValidateConnection`, `Broadcast(...)`, and `SendTo(...)`.
- **Dual Consumption API:** Zero-allocation `TryPollEvent(...)` / `PollEvents(...)` for fixed-tick loops, plus `ReadEventsAsync(...)` and `Events` async delegates.
- **Adaptive Congestion & Time Sync:** Jacobson/Karels SRTT/RTTVAR/RTO, fast retransmit, token-bucket byte pacing, and NTP-style `TimeSyncPing`/`Pong` clock-offset estimation.
- **Single-Socket NAT Hole Punching:** Built-in `DatagramRendezvousServer`, `DiscoverPublicEndPointAsync`, `RequestIntroduction`, `PunchAsync`, and `PunchAndConnectAsync` sharing the host's bound socket.
- **Deterministic Chaos Testing:** Built-in `SimulatedNetworkHub`, `SimulatedDatagramTransport`, `SimulatedChaosProfile`, and `SimulatedTimeProvider` for reproducible packet loss, jitter, reordering, duplication, corruption, and NAT migration tests.

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

---

## 3. Event Dispatch & Tick Modes

`DatagramHost` supports two orthogonal execution choices: **how ticks are driven** (`ManualTick`) and **how events are consumed** (`DispatchMode`).

### Polling Inside a Fixed-Tick Game Loop (`ManualTick = true`, `DispatchMode = Poll`)
Ideal for 30-120 Hz simulations where you want zero background callback races and full control over when packets flush:

```csharp
var options = new DatagramOptions
{
   ManualTick = true,
   DispatchMode = DatagramEventDispatchMode.Poll
}
.AddChannel(0, DatagramDeliveryMode.ReliableOrdered);

await using var host = new DatagramHost(new IPEndPoint(IPAddress.Any, 7777), options);
host.Start();

// Inside your fixed-rate tick loop:
host.Tick();
host.PollEvents(evt =>
{
   if (evt.Type == DatagramEventType.MessageReceived)
   {
      HandlePacket(evt.Peer, evt.ChannelId, evt.Payload.Span);
   }
});
```

> [!IMPORTANT]
> - **`PollEvents(Action<DatagramEvent>)`** automatically disposes every `DatagramEvent` after your callback returns. If you use **`TryPollEvent(out var evt)`** directly, wrap it in `using (evt)` so rented `ArrayPool<byte>` buffers are returned.
> - When **`ManualTick = true`**, handshake retries are driven by `host.Tick()`. While awaiting `ConnectAsync(...)` over a lossy network, keep pumping `host.Tick()` periodically until the returned `Task<DatagramPeer>` completes.

### Background Async Mode (`ManualTick = false`, `DispatchMode = Async`)
Ideal for dedicated servers, lobbies, and services:

```csharp
var options = new DatagramOptions
{
   ManualTick = false,
   TickInterval = TimeSpan.FromMilliseconds(10),
   DispatchMode = DatagramEventDispatchMode.Async
}
.AddChannel(0, DatagramDeliveryMode.ReliableOrdered);

await using var server = new DatagramHost(new IPEndPoint(IPAddress.Any, 9000), options);

// Register Events callbacks BEFORE calling Start()!
server.Events.PeerConnected = (peer, _) =>
{
   Console.WriteLine($"Peer #{peer.PeerId} connected from {peer.RemoteEndPoint}");
   return ValueTask.CompletedTask;
};
server.Events.MessageReceived = (peer, channelId, mode, payload, _) =>
{
   server.Broadcast(channelId, payload.Span, exclude: peer);
   return ValueTask.CompletedTask;
};

server.Start();
```

Alternatively, leave `Events.PeerConnected` / `PeerDisconnected` / `MessageReceived` / `PeerMigrated` unset and consume events with `await foreach`:

```csharp
await foreach (var evt in server.ReadEventsAsync(ct))
{
   using (evt)
   {
      // Handle evt.Type, evt.Peer, evt.ChannelId, evt.Payload.Span
   }
}
```

> [!NOTE]
> In `DatagramEventDispatchMode.Async`, either register `Events.*` delegates **before** `Start()` / `ConnectAsync()`, **or** read via `ReadEventsAsync(ct)` - do not mix both on the same `DatagramHost` because they share the same internal event channel.

---

## 4. Security, Admission Control & Endpoint Migration

Configure packet protection via `DatagramOptions.Security`:

| `DatagramSecurityMode` | Overhead | Protection |
| :--- | :---: | :--- |
| **`Aead`** (default) | `16` bytes | Ephemeral ECDH P-256 + HKDF-SHA256 + optional `PreSharedKey`, per-packet `AesGcm` / `ChaCha20Poly1305` encryption & AAD header authentication |
| **`Crc32C`** | `4` bytes | Castagnoli CRC-32C integrity check without encryption |
| **`None`** | `0` bytes | No checksum or encryption; best suited for local loopback benchmarks |

- **Admission Control (`Events.ValidateConnection`):** Runs on the server after cookie verification and metadata decryption. Attach typed session state via `ctx.PeerTag` or reject with an encrypted reason payload via `ctx.Reject("Account banned"u8)` (throws `DatagramConnectionRejectedException` on the client). Async validation callbacks run off the UDP receive loop so slow lookups never stall traffic for connected peers.
- **Anti-Amplification (`PadConnectRequests = true`):** Pads `ConnectRequest` packets to `MaxDatagramSize` so unverified senders cannot trigger larger `ConnectChallenge` responses.
- **Endpoint Mobility (`AllowEndpointMigration = true`):** When a client's NAT mapping or Wi-Fi/cellular IP changes mid-session, any authenticated packet with a strictly newer `PacketSeq` migrates the peer seamlessly and emits `DatagramEventType.PeerMigrated`.
- **Dual-Stack IPv6:** Binding a `DatagramHost` to `IPAddress.IPv6Any` automatically enables `Socket.DualMode` so both IPv4 and IPv6 peers can connect cleanly.

---

## 5. Multi-Peer Routing, Congestion, Clock Sync & Telemetry

- **Targeted & Broadcast Sending:**
  - `host.Broadcast(channelId, payload, exclude: senderPeer)` sends to all connected peers.
  - `host.SendTo(peerIdsSpan, channelId, payload)` sends to a specific squad/room subset.
  - `peer.Tag` and `peer.SetProperty<T>` / `TryGetProperty<T>` store per-peer application state.
  - `peer.Disconnect("reason"u8)` transmits redundant graceful disconnect notices before releasing peer state.
- **Link Stats & Congestion Signaling (`peer.Stats`):**
  - `SmoothedRttMs`, `RttVarianceMs`, `CurrentRtoMs`, `PacketLossRate`, `MessagesRetransmitted`, `UnackedReliableMessages`, `BytesSent`, `BytesReceived`.
  - `CongestionState` (`Healthy`, `Congested`, `Critical`) and `RecommendedSendScale` (`1.0f`, `0.5f`, `0.25f`) let your game loop dynamically scale snapshot/input send rates under packet loss or RTT inflation.
  - `EstimatedClockOffsetTicks` and `peer.EstimatedRemoteUtcNow` track NTP-style clock offset automatically when `TimeSyncInterval > TimeSpan.Zero`.
- **OpenTelemetry / `System.Diagnostics.Metrics`:** Subscribe to meter `"Beskar.Networking.Datagram"` (`DatagramMetrics.MeterName`). See the full instrument list in the [Telemetry Meters Reference](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Documentation/Telemetry/Meters.md#datagram).

---

## 6. Single-Socket NAT Hole Punching

`DatagramHost` can discover its NAT-mapped public endpoint, receive simultaneous introductions from a lightweight `DatagramRendezvousServer`, punch UDP pinholes, and establish an authenticated AEAD session - all on **one bound UDP socket**:

```csharp
// 1. Public rendezvous server
await using var rendezvous = new DatagramRendezvousServer(new IPEndPoint(IPAddress.Any, 9500));
rendezvous.Start();

// 2. Both NAT-ed peers discover their public endpoints
var pubA = await hostA.DiscoverPublicEndPointAsync(rendezvousEp, "peer-a"u8.ToArray());
var pubB = await hostB.DiscoverPublicEndPointAsync(rendezvousEp, "peer-b"u8.ToArray());

// 3. Coordinate simultaneous introduction via Events.PunchIntroduced
hostA.RequestIntroduction(rendezvousEp, "peer-a"u8, "peer-b"u8, punchToken: 0xCAFE_1234UL);

// 4. Both sides punch simultaneously; initiator opens AEAD session on the punched pinhole
var punchResult = await hostB.PunchAsync(introB.PeerPublicEndPoint, introB.PeerLanEndPoint, introB.PunchToken);
var peerOnA = await hostA.PunchAndConnectAsync(introA.PeerPublicEndPoint, introA.PeerLanEndPoint, introA.PunchToken);
// If punchResult.NeedsRelayFallback is true (e.g., symmetric NAT), fall back to a relay server.
```

---

## 7. Deterministic Chaos Testing

Pass `SimulatedNetworkHub.CreateTransport()` and `hub.Time` (`SimulatedTimeProvider`) into `DatagramHost` with `ManualTick = true` to deterministically test loss, jitter, reordering, duplication, bit-flips, and NAT rebinds with zero flakiness:

```csharp
var hub = new SimulatedNetworkHub(seed: 42, chaos: new SimulatedChaosProfile
{
   PacketDropRate = 0.15,
   PacketDuplicateRate = 0.10,
   ReorderRate = 0.20,
   DataCorruptionRate = 0.02,
   MinLatency = TimeSpan.FromMilliseconds(10),
   MaxLatency = TimeSpan.FromMilliseconds(50)
});

var server = new DatagramHost(serverEp, opts, hub.CreateTransport(), hub.Time);
var clientTransport = hub.CreateTransport();
var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), opts, clientTransport, hub.Time);

// Simulate a live Wi-Fi -> 5G NAT migration mid-session:
hub.MigrateEndpoint(clientTransport, new IPEndPoint(IPAddress.Parse("10.0.0.42"), 55555));
```

---

## 8. Included Examples

Progress from basic usage to a full lag-compensated 2D game:

1. [**Multi-Peer Server (Beginner)**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Datagram/Beskar.Datagram.Example.MultiPeerServer): Minimal 65-line server + 2 clients showing metadata validation, peer tags, and channel broadcasting.
2. [**Co-op Match Lobby (Intermediate)**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Datagram/Beskar.Datagram.Example.CoopMatchLobby): Cross-platform walkthrough of all 5 delivery modes, 16 KB multi-MTU fragmentation under WAN chaos, `SendTo` subsets, mid-match NAT migration, adaptive congestion, clock sync, `DatagramMetrics`, and real-UDP NAT hole punching.
3. [**Console Arena Game (Intermediate-Advanced)**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Datagram/Beskar.Datagram.Example.ArenaGame): Playable 30 Hz terminal listen-server + real UDP clients with AEAD + PSK and 4 channels (`host`, `join`, or `demo` modes).
4. [**2D Arena Game (Advanced - Windows)**](https://github.com/MarvinDrude/Beskar.Networking/tree/master/Examples/Datagram/Beskar.Datagram.Example.ArenaGame2D): Full 60 Hz WinForms 2D multiplayer shooter with client-side prediction, server reconciliation, snapshot interpolation, 6x redundant input packing, bounded hitbox-rewind lag compensation, and live UDP fault injection.
