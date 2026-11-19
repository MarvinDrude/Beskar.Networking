# `Beskar.Networking.Datagram` - Delivery & Reliability Modes

`Beskar.Networking.Datagram` multiplexes up to **64 independent logical channels (`0..63`)** into shared, MTU-safe UDP datagrams (`MaxDatagramSize`, default `1200` bytes). Each channel is configured with one of **5 delivery modes** (`DatagramDeliveryMode`), allowing unreliable high-frequency telemetry and strictly ordered commands to share the same connection **without blocking each other**.

For general setup, architecture, and security boundaries, see the [Datagram Overview](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Documentation/Datagram/Overview.md).

---

## 1. At-a-Glance Comparison

| Delivery Mode | Guaranteed Delivery? | Ordering Guarantee | Drops Allowed? | Head-of-Line Blocking? | Per-Message Seq Overhead | Ideal For |
| :--- | :---: | :---: | :---: | :---: | :---: | :--- |
| **`UnreliableUnordered`** | No | None | Yes | No | `0` bytes | Ephemeral FX, non-critical audio cues, fire-and-forget pings |
| **`UnreliableSequenced`** | No | Latest-only (drops stale) | Yes | No | `2` bytes | Continuous transforms, physics states, live sensor streams, player inputs |
| **`ReliableUnordered`** | Yes | None (deduplicated) | No | No | `2` bytes | Independent discrete events, pickups, hit confirmations, independent telemetry alerts |
| **`ReliableOrdered`** | Yes | Strict FIFO per channel | No | Within same channel only | `2` bytes | State machines, chat, RPCs, scene/phase transitions, turn actions |
| **`ReliableSequenced`** | Latest only | Latest-only (drops stale) | Older superseded dropped | No | `2` bytes | Intermittent full-state snapshots that must converge (HUD stats, ready toggles, config state) |

---

## 2. Detailed Breakdown & Use Cases

### 1. `UnreliableUnordered` (Fire & Forget)
- **How it works:** Messages carry no channel sequence ID, are never retransmitted, and are delivered immediately upon receipt. If a datagram drops or arrives out of order, it is simply delivered whenever (or if) it arrives.
- **Scenario (`1, 2, 3` sent; `2` drops, `3` arrives before `1`):** Receiver gets `3`, then `1`. `2` is never resent.
- **When to use it:**
  - **Multiplayer games:** One-off cosmetic particle sparks, footstep/explosion sound triggers, ambient visual effects.
  - **Telemetry & real-time apps:** Non-critical debug counters, heartbeat beacons, redundant broadcast announcements where missing one packet has zero impact.

```csharp
options.AddChannel(
   channelId: 3,
   deliveryMode: DatagramDeliveryMode.UnreliableUnordered,
   priority: 50,
   allowFragmentation: false);
```

---

### 2. `UnreliableSequenced` (Latest-Only, Best Effort)
- **How it works:** Each message carries a 16-bit wrap-safe `ChannelSeq`. No retransmissions occur. The receiver tracks the newest sequence delivered so far; any older packet arriving late is **silently dropped** so stale state never overwrites fresh state.
- **Scenario (`1, 2, 3` sent; `2` arrives after `3`):** Receiver gets `1`, then `3`, and drops `2`. If `3` drops and no `4` is sent, `3` is not retransmitted.
- **When to use it:**
  - **Multiplayer games:** High-frequency entity transforms (20-60 Hz positions/rotations/velocities), physics ragdolls, projectile updates, countdown timers, and client input streams (especially paired with redundant recent-tick input packing).
  - **Robotics / Telemetry / Streaming:** Live GPS/IMU coordinates, drone attitude updates, live dashboard gauges, or real-time control stick feeds where a 100 ms old value is worse than a dropped packet.

```csharp
options.AddChannel(
   channelId: 2,
   deliveryMode: DatagramDeliveryMode.UnreliableSequenced,
   priority: 150,
   allowFragmentation: false);
```

---

### 3. `ReliableUnordered` (Guaranteed Delivery, Zero Ordering Stalls)
- **How it works:** Every message is tracked in the sender's sliding window and retransmitted (via piggybacked ACKs, RTO backoff, and fast retransmit) until acknowledged. The receiver uses a sliding deduplication bitset and delivers each message **immediately on first arrival** - never stalling waiting for an earlier dropped message.
- **Scenario (`1, 2, 3` sent; `2` drops initially):** Receiver immediately delivers `1`, then immediately delivers `3` without waiting, then delivers `2` once its retransmission arrives. Duplicates are ignored.
- **When to use it:**
  - **Multiplayer games:** Independent gameplay events where order between unrelated items does not matter - coin/key pickups, individual player damage/elimination notifications, crate breaks, independent score deltas, achievement triggers.
  - **Distributed / Telemetry apps:** Independent device fault alerts, fire-and-confirm sensor readings, or parallel RPC responses keyed by their own correlation ID.

```csharp
options.AddChannel(
   channelId: 1,
   deliveryMode: DatagramDeliveryMode.ReliableUnordered,
   priority: 200,
   windowSize: 512,
   allowFragmentation: true);
```

---

### 4. `ReliableOrdered` (Guaranteed Strict FIFO per Channel)
- **How it works:** Every message is retransmitted until acknowledged, and the receiver buffers out-of-order arrivals in a sliding reorder ring buffer until every earlier gap is filled, delivering messages strictly in send order (`1 → 2 → 3`).
- **Scenario (`1, 2, 3` sent; `2` drops initially):** Receiver delivers `1`, buffers `3`, waits for `2` to retransmit, then delivers `2` and `3` in exact order.
- **Avoiding Head-of-Line Blocking:** Because channels are completely isolated, put unrelated ordered streams on **separate `ReliableOrdered` channels** (e.g., match lifecycle on Channel `0`, chat on Channel `5`) so a lost chat packet never delays a scene transition.
- **When to use it:**
  - **Multiplayer games:** Match/round lifecycle transitions, turn-based state machines (roll dice → choose path → buy item → end turn), lobby join/leave sequences, chat messages, reliable ordered RPCs, full-state reconnect snapshots.
  - **Control & Automation:** Sequential command pipelines, remote terminal/CLI streams, ordered configuration deltas, or multi-step workflows where step `N+1` depends on step `N`.

```csharp
options.AddChannel(
   channelId: 0,
   deliveryMode: DatagramDeliveryMode.ReliableOrdered,
   priority: 255,
   windowSize: 512,
   allowFragmentation: true);
```

---

### 5. `ReliableSequenced` (Guaranteed Latest-Only State Convergence)
- **How it works:** Combines reliability with latest-only superseding. When a newer message is queued on the sender before an older message is acknowledged, the older unacked message is **discarded and never retransmitted** - only the newest message keeps retrying until acknowledged. On the receiver, stale out-of-order arrivals (`seq <= latest`) are dropped immediately.
- **Scenario (`1 ("HP=100")`, `2 ("HP=80")`, `3 ("HP=60")` sent; `2` drops):** Receiver delivers `1`, then delivers `3` immediately (skipping `2`). If `3` also drops and no `4` is ever sent, the sender keeps retransmitting `3` until the receiver converges on `"HP=60"`.
- **How it differs from `ReliableOrdered` and `UnreliableSequenced`:**
  - Unlike **`ReliableOrdered`**, it does not waste bandwidth retransmitting obsolete intermediate states and never head-of-line blocks on them.
  - Unlike **`UnreliableSequenced`**, if updates stop after the last packet drops, the receiver will **never get stuck** on an outdated value.
- **When to use it:**
  - **Multiplayer games:** Lobby `Ready` / `Unready` toggles, authoritative HUD summaries (current HP, coins, keys, equipped item slot), match scoreboard totals, or UI selection states.
  - **IoT / Telemetry / Control:** Desired actuator target setpoints (e.g., thermostat target temperature, valve open/closed state, LED color mode) where intermediate values are irrelevant once superseded, but the final state *must* eventually take effect.

```csharp
options.AddChannel(
   channelId: 4,
   deliveryMode: DatagramDeliveryMode.ReliableSequenced,
   priority: 180,
   windowSize: 128,
   allowFragmentation: true);
```

---

## 3. Recommended Multi-Channel Blueprint

You can combine all 5 modes on a single `DatagramHost` and tune **`Priority`** (`0..255`, higher packed first) so critical packets always claim MTU space ahead of high-frequency snapshots:

```csharp
var options = new DatagramOptions
{
   MaxDatagramSize = 1200,
   DispatchMode = DatagramEventDispatchMode.Poll,
   ManualTick = true
}
// Highest priority: strict match/session lifecycle & turn state
.AddChannel(0, DatagramDeliveryMode.ReliableOrdered, priority: 255)
// High priority: independent discrete events (pickups, hits, alerts)
.AddChannel(1, DatagramDeliveryMode.ReliableUnordered, priority: 220)
// Medium-high priority: authoritative latest-only HUD / toggle state
.AddChannel(2, DatagramDeliveryMode.ReliableSequenced, priority: 180)
// Medium priority: high-frequency snapshots / positions / sensor streams
.AddChannel(3, DatagramDeliveryMode.UnreliableSequenced, priority: 140, allowFragmentation: false)
// Lower priority: isolated ordered chat/social stream (cannot stall channel 0)
.AddChannel(4, DatagramDeliveryMode.ReliableOrdered, priority: 90)
// Lowest priority: ephemeral cosmetic FX / audio cues
.AddChannel(5, DatagramDeliveryMode.UnreliableUnordered, priority: 40, allowFragmentation: false);
```

### Practical Guidelines
1. **Keep `allowFragmentation: false` on unreliable channels** unless payloads strictly require it - losing 1 of 4 fragments over UDP wastes all 4 fragments. Fragmentation shines on `ReliableOrdered`, `ReliableUnordered`, and `ReliableSequenced` (e.g., initial full-state sync blobs or lobby thumbnails).
2. **Isolate unrelated ordered streams onto separate `ReliableOrdered` channels** so packet loss in low-priority traffic (like chat) never delays critical gameplay/control messages.
3. **Match `DatagramOptions.Channels` on both peers:** During the handshake, `ChannelConfigHash` is verified automatically so mismatched channel layouts fail fast instead of silently misinterpreting frames.
