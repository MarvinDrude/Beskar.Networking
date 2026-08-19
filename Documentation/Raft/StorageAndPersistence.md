# Raft Storage & Persistence Guide

In Raft, persistent state is vital for safety across node restarts. If a node forgets which term it is on or who it voted for, split-brain scenarios and multiple leaders could emerge in the same term.

`Beskar.Networking.Raft` abstracts log and term storage behind [`IRaftLogStorage`](https://github.com/MarvinDrude/Beskar.Networking/blob/master/Raft/Beskar.Networking.Raft/Storage/IRaftLogStorage.cs) and provides two ready-to-use implementations.

---

## 1. Storage Implementations

| Storage Engine | Class | Best For | Persistence |
| :--- | :--- | :--- | :---: |
| **In-Memory** | `InMemoryRaftLogStorage` | Unit testing, chaos simulations, ephemeral clusters | No |
| **Disk-Backed WAL** | `FileRaftLogStorage` | Production deployments, durable state machines | **Yes (Crash-Safe)** |

---

## 2. `FileRaftLogStorage` Structure

`FileRaftLogStorage` stores data in a dedicated directory using two files:

```
data-directory/
├── metadata.bin     # Atomic binary state: CurrentTerm (8B) + VotedFor (string)
└── raft.log         # Append-only binary log: [Term(8B) | Index(8B) | Length(4B) | Payload]
```

### Crash-Safe Guarantees
* **Metadata Writes**: Flushed synchronously to disk using `FileStream.Flush(flushToDisk: true)` or atomic overwrites upon term increment or voting.
* **Append-Only Performance**: Log entries are appended sequentially to `raft.log`. In-memory index offsets allow $O(1)$ lookup time for any log entry index without scanning the disk.
* **Corrupt Tail Recovery**: On startup, if a partial or torn write occurred during an abrupt power loss, `FileRaftLogStorage` scans the valid prefix and truncates any incomplete trailing byte sequence automatically.

---

## 3. Log Truncation Semantics

Log truncation (`TruncateLogAsync(ulong fromIndex)`) happens automatically under two specific consensus conditions:

1. **Follower Conflict Overwrite**: When an uncommitted leader was superseded and a new authoritative leader sends entries starting from an earlier index with a new term, conflicting uncommitted entries from `fromIndex` onwards are truncated.
2. **Snapshot Compaction**: When state is compacted into a snapshot, all entries preceding `fromIndex` are trimmed to reclaim disk space.

---

## 4. Usage Example

```csharp
using Beskar.Networking.Raft.Storage;

// 1. Initialize persistent storage pointing to a directory
var storage = new FileRaftLogStorage("./cluster-data/node-alpha");

// 2. Query persistent state on startup
var currentTerm = await storage.GetCurrentTermAsync();
var lastIndex = await storage.GetLastLogIndexAsync();
Console.WriteLine($"Recovered node state: Term {currentTerm}, Last Log Index #{lastIndex}");
```
