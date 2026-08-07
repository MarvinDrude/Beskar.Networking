# Telemetry Meters & Instruments

This document outlines all OpenTelemetry/`System.Diagnostics.Metrics` meters, instruments, types, units, and descriptions implemented across Beskar.Networking.

---

# Mqtt

Meter Name: `Beskar.Mqtt` (Version `1.0.0`)

| Instrument Name | Type | Unit | Description |
| :--- | :--- | :--- | :--- |
| `beskar.mqtt.server.clients.connected` | `UpDownCounter<long>` | `{client}` | Current count of active connected MQTT clients. |
| `beskar.mqtt.server.sessions.active` | `UpDownCounter<long>` | `{session}` | Current count of active stored MQTT client sessions. |
| `beskar.mqtt.subscriptions.active` | `UpDownCounter<long>` | `{subscription}` | Current count of active MQTT subscriptions in trie router. |
| `beskar.mqtt.retained_messages.active` | `UpDownCounter<long>` | `{message}` | Current count of active retained MQTT messages stored in trie. |
| `beskar.mqtt.messages.published` | `Counter<long>` | `{message}` | Total number of MQTT PUBLISH messages transmitted or received. |
| `beskar.mqtt.qos.inflight` | `UpDownCounter<long>` | `{message}` | Current count of QoS 1 and QoS 2 in-flight unacknowledged messages. |
| `beskar.mqtt.qos.retries` | `Counter<long>` | `{retry}` | Total number of QoS 1 and QoS 2 message retransmissions. |
| `beskar.mqtt.topic_alias.hits` | `Counter<long>` | `{hit}` | Total number of topic alias resolution cache hits. |
| `beskar.mqtt.last_will.triggered` | `Counter<long>` | `{will}` | Total number of Last Will and Testament messages triggered upon ungraceful client disconnects. |

---

# Resilient

Meter Name: `Beskar.Networking.Resilient` (Version `1.0.0`)

| Instrument Name | Type | Unit | Description |
| :--- | :--- | :--- | :--- |
| `beskar.resilient.sessions.active` | `UpDownCounter<long>` | `{session}` | Current count of active resilient sessions. |
| `beskar.resilient.reconnect.attempts` | `Counter<long>` | `{attempt}` | Total number of auto-reconnection attempts. |
| `beskar.resilient.reconnect.duration` | `Histogram<double>` | `ms` | Duration of reconnection attempts in milliseconds. |
| `beskar.resilient.ping.rtt` | `Histogram<double>` | `ms` | Round-trip time of keep-alive pings in milliseconds. |
| `beskar.resilient.ping.timeouts` | `Counter<long>` | `{timeout}` | Total number of keep-alive ping timeouts. |
| `beskar.resilient.auth.attempts` | `Counter<long>` | `{attempt}` | Total number of authentication handshake attempts. |
| `beskar.resilient.offline_queue.size` | `UpDownCounter<long>` | `{frame}` | Current count of buffered frames in the offline queue. |
| `beskar.resilient.offline_queue.dropped` | `Counter<long>` | `{frame}` | Total number of frames dropped due to offline queue overflow. |

---

# Transports

Meter Name: `Beskar.Networking.Transport` (Version `1.0.0`)

| Instrument Name | Type | Unit | Description |
| :--- | :--- | :--- | :--- |
| `beskar.transport.connections.active` | `UpDownCounter<long>` | `{connection}` | Current count of active open network connections. |
| `beskar.transport.connections.opened` | `Counter<long>` | `{connection}` | Total number of network connections established. |
| `beskar.transport.connections.closed` | `Counter<long>` | `{connection}` | Total number of network connections closed. |
| `beskar.transport.streams.active` | `UpDownCounter<long>` | `{stream}` | Current count of active open network streams. |
| `beskar.transport.streams.opened` | `Counter<long>` | `{stream}` | Total number of network streams opened. |
| `beskar.transport.streams.closed` | `Counter<long>` | `{stream}` | Total number of network streams closed. |
| `beskar.transport.bytes.sent` | `Counter<long>` | `By` | Total bytes sent over network pipelines. |
| `beskar.transport.bytes.received` | `Counter<long>` | `By` | Total bytes received over network pipelines. |
| `beskar.transport.packets.sent` | `Counter<long>` | `{packet}` | Total frames/packets sent over transport streams. |
| `beskar.transport.packets.received` | `Counter<long>` | `{packet}` | Total frames/packets received over transport streams. |
