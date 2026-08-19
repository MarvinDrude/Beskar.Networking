using System.Collections.Concurrent;
using System.Text;
using System.Text.Json;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Raft;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;
using Beskar.Utilities.Console.Rendering;
using Beskar.Utilities.Tracing;

namespace Beskar.Raft.ChaosSimulator;

public static class Program
{
   private static readonly Lock LogLock = new();

   internal static int Profile { get; set; } = 1; // 1 = Standard, 2 = High Throughput, 3 = Partitions, 4 = Snapshots
   internal static int ClusterSize { get; set; } = 3; // 3, 5, 7
   internal static bool UseFileStorage { get; set; } = false;
   internal static int TargetProposalsPerSec { get; set; } = 50;
   internal static int StatsIntervalSeconds { get; set; } = 3;

   // Cluster Metrics Counters
   internal static long ProposalsSubmitted;
   internal static long ProposalsCommitted;
   internal static long ProposalsFailed;
   internal static long ElectionsCount;
   internal static long CrashesInjected;
   internal static long RestartsInjected;
   internal static long PartitionsInjected;
   internal static long PartitionsHealed;
   internal static long SnapshotsTaken;

   private static readonly ConcurrentDictionary<string, RaftNodeHandle> ClusterNodes = new();
   private static readonly ConcurrentDictionary<string, bool> PartitionedNodes = new();

   public static async Task Main(string[] args)
   {
      TraceLogger.IsEnabled = false;

      try
      {
         Console.Clear();
      }
      catch
      {
         // ignored
      }

      ConsoleRender.DrawHeader("BESKAR RAFT CHAOS SIMULATOR",
         "Simulating Raft consensus engine under network partitions, node crashes, and log compaction");

      Console.WriteLine("Select Chaos Profile:");
      Console.WriteLine("  1. Standard Raft Chaos (Random node crashes, partitions, proposals)");
      Console.WriteLine("  2. High Throughput & Low Disruption (High proposal rate, stable leadership)");
      Console.WriteLine("  3. Partition & Split-Brain Chaos (Aggressive network isolates & heals)");
      Console.WriteLine("  4. Snapshot & Catch-Up Chaos (Frequent log compaction & snapshot catch-up)");
      Profile = PromptInt("Profile", Profile);

      Console.WriteLine("\nConfigure Cluster Settings:");
      ClusterSize = PromptInt("Cluster Size (3, 5, 7)", ClusterSize);
      if (ClusterSize < 3) ClusterSize = 3;
      if (ClusterSize % 2 == 0) ClusterSize++; // Ensure odd number of nodes for clean majority

      var storageChoice = PromptInt("Storage Type (1 = In-Memory, 2 = Persistent File Storage)", 1);
      UseFileStorage = storageChoice == 2;

      TargetProposalsPerSec = PromptInt("Target Proposals / Second", TargetProposalsPerSec);
      StatsIntervalSeconds = PromptInt("Stats reporting interval (seconds)", StatsIntervalSeconds);

      Console.WriteLine();
      ConsoleRender.Success($"Starting Raft Chaos Simulator ({ClusterSize}-node cluster)...");
      ConsoleRender.Info($"Storage: {(UseFileStorage ? "Persistent File Storage" : "In-Memory Storage")}");

      using var cts = new CancellationTokenSource();

      Console.CancelKeyPress += (_, e) =>
      {
         e.Cancel = true;
         cts.Cancel();
      };

      // 1. Initialize Cluster Nodes & Transports
      var nodeIds = Enumerable.Range(1, ClusterSize).Select(i => $"node-{i}").ToList();
      var memoryOptions = new MemoryTransportOptions();

      var tempBaseDir = Path.Combine(Path.GetTempPath(), $"raft_chaos_{Guid.NewGuid():N}");
      if (UseFileStorage) Directory.CreateDirectory(tempBaseDir);

      try
      {
         // CreateEndpoints & Transports with Chaos Middleware
         var endpoints = nodeIds.ToDictionary(id => id, id => new MemoryEndPoint($"raft-chaos-{id}-{Guid.NewGuid():N}"));

         foreach (var id in nodeIds)
         {
            var peers = nodeIds.Where(x => x != id).ToList();

            var peerEndpoints = peers.Select(p => new RaftPeerEndpoint(
               p,
               endpoints[p],
               () => new MemoryNetworkClient(memoryOptions))).ToList();

            var listener = new MemoryNetworkListener(endpoints[id], memoryOptions);
            var transport = new PartitionableRaftTransport(listener, peerEndpoints, id, PartitionedNodes);

            var options = new RaftNodeOptions
            {
               NodeId = id,
               Peers = peers,
               ElectionTimeoutMin = TimeSpan.FromMilliseconds(150),
               ElectionTimeoutMax = TimeSpan.FromMilliseconds(300),
               HeartbeatInterval = TimeSpan.FromMilliseconds(40)
            };

            IRaftLogStorage storage = UseFileStorage
               ? new FileRaftLogStorage(Path.Combine(tempBaseDir, id))
               : new InMemoryRaftLogStorage();

            var stateMachine = new ChaosStateMachine();

            var node = new RaftNode(options, storage, stateMachine, transport);
            node.Events.OnRoleChanged.Add((ctx, _) =>
            {
               if (ctx.NewRole == RaftRole.Leader) Interlocked.Increment(ref ElectionsCount);
               return ValueTask.CompletedTask;
            });

            ClusterNodes[id] = new RaftNodeHandle(id, node, storage, stateMachine, transport, options);
         }

         // Start all nodes
         foreach (var handle in ClusterNodes.Values)
         {
            await handle.Node.StartAsync(cts.Token);
         }

         // 2. Launch Background Tasks: Proposals, Chaos Invalidation, Stats Loop
         var proposalTask = Task.Run(() => RunProposalWorkloadLoopAsync(cts.Token));
         var chaosTask = Task.Run(() => RunChaosInjectorLoopAsync(cts.Token));
         var statsTask = Task.Run(() => RunStatsLoopAsync(cts.Token));

         await Task.WhenAny(proposalTask, chaosTask, statsTask);
      }
      finally
      {
         cts.Cancel();

         foreach (var handle in ClusterNodes.Values)
         {
            try
            {
               await handle.Node.DisposeAsync();
            }
            catch
            {
               // ignored
            }
         }

         if (UseFileStorage && Directory.Exists(tempBaseDir))
         {
            try
            {
               Directory.Delete(tempBaseDir, recursive: true);
            }
            catch
            {
               // ignored
            }
         }
      }
   }

   private static async Task RunProposalWorkloadLoopAsync(CancellationToken ct)
   {
      ulong counter = 0;

      while (!ct.IsCancellationRequested)
      {
         try
         {
            var delayMs = Math.Max(1, 1000 / TargetProposalsPerSec);
            await Task.Delay(delayMs, ct);

            // Find current leader node
            var leaderHandle = ClusterNodes.Values.FirstOrDefault(h => h.Node.Role == RaftRole.Leader && !h.IsCrashed && !PartitionedNodes.ContainsKey(h.NodeId));

            if (leaderHandle == null)
            {
               continue; // No leader available due to chaos
            }

            var key = $"sensor_{counter % 20}";
            var val = $"val_{counter}";
            counter++;

            Interlocked.Increment(ref ProposalsSubmitted);

            using var proposalCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            proposalCts.CancelAfter(1500);

            var cmd = Encoding.UTF8.GetBytes($"SET {key}={val}");
            var result = await leaderHandle.Node.ProposeAsync(cmd, proposalCts.Token);

            if (!result.IsEmpty)
            {
               Interlocked.Increment(ref ProposalsCommitted);
            }
         }
         catch
         {
            Interlocked.Increment(ref ProposalsFailed);
         }
      }
   }

   private static async Task RunChaosInjectorLoopAsync(CancellationToken ct)
   {
      while (!ct.IsCancellationRequested)
      {
         try
         {
            var interval = Profile switch
            {
               2 => TimeSpan.FromSeconds(8),  // Low disruption
               3 => TimeSpan.FromSeconds(3),  // Frequent partitions
               4 => TimeSpan.FromSeconds(4),  // Snapshot & compaction
               _ => TimeSpan.FromSeconds(5)   // Standard
            };

            await Task.Delay(interval, ct);

            var rnd = Random.Shared.Next(100);

            if (Profile == 3 || rnd < 40) // Inject Network Partition
            {
               var targetNode = ClusterNodes.Keys.ElementAt(Random.Shared.Next(ClusterNodes.Count));
               if (PartitionedNodes.ContainsKey(targetNode))
               {
                  PartitionedNodes.TryRemove(targetNode, out _);
                  Interlocked.Increment(ref PartitionsHealed);
               }
               else
               {
                  PartitionedNodes[targetNode] = true;
                  Interlocked.Increment(ref PartitionsInjected);
               }
            }
            else if (Profile == 4 || rnd < 75) // Log Compaction & Snapshot
            {
               var targetHandle = ClusterNodes.Values.ElementAt(Random.Shared.Next(ClusterNodes.Count));
               if (targetHandle.Node.Role != RaftRole.Stopped)
               {
                  var lastApplied = targetHandle.Node.LastApplied;
                  if (lastApplied > 5)
                  {
                     var compactIndex = lastApplied - 2;
                     await targetHandle.Storage.CompactPrefixAsync(compactIndex, targetHandle.Node.CurrentTerm, ct);
                     Interlocked.Increment(ref SnapshotsTaken);
                  }
               }
            }
            else // Node Crash / Restart
            {
               var targetHandle = ClusterNodes.Values.ElementAt(Random.Shared.Next(ClusterNodes.Count));
               if (targetHandle.IsCrashed)
               {
                  await targetHandle.Node.StartAsync(ct);
                  targetHandle.IsCrashed = false;
                  Interlocked.Increment(ref RestartsInjected);
               }
               else
               {
                  await targetHandle.Node.StopAsync(ct);
                  targetHandle.IsCrashed = true;
                  Interlocked.Increment(ref CrashesInjected);
               }
            }
         }
         catch
         {
            // Chaos injection loop catch
         }
      }
   }

   private static async Task RunStatsLoopAsync(CancellationToken ct)
   {
      var lastTime = DateTime.UtcNow;
      long lastSubmitted = 0;
      long lastCommitted = 0;

      while (!ct.IsCancellationRequested)
      {
         try
         {
            await Task.Delay(TimeSpan.FromSeconds(StatsIntervalSeconds), ct);

            var now = DateTime.UtcNow;
            var elapsedSec = (now - lastTime).TotalSeconds;
            lastTime = now;

            var currSubmitted = Volatile.Read(ref ProposalsSubmitted);
            var currCommitted = Volatile.Read(ref ProposalsCommitted);

            var submittedPerSec = (currSubmitted - lastSubmitted) / elapsedSec;
            var committedPerSec = (currCommitted - lastCommitted) / elapsedSec;

            lastSubmitted = currSubmitted;
            lastCommitted = currCommitted;

            lock (LogLock)
            {
               Console.WriteLine($"\n--- [RAFT CHAOS DASHBOARD @ {DateTime.Now:HH:mm:ss}] ---");
               Console.WriteLine($"  Proposals: Submitted = {currSubmitted:N0} ({submittedPerSec:F1}/s) | Committed = {currCommitted:N0} ({committedPerSec:F1}/s) | Failed = {Volatile.Read(ref ProposalsFailed):N0}");
               Console.WriteLine($"  Chaos Metrics: Elections = {Volatile.Read(ref ElectionsCount)} | Crashes = {Volatile.Read(ref CrashesInjected)} | Restarts = {Volatile.Read(ref RestartsInjected)} | Partitions = {Volatile.Read(ref PartitionsInjected)} | Heals = {Volatile.Read(ref PartitionsHealed)} | Snapshots = {Volatile.Read(ref SnapshotsTaken)}");

               Console.WriteLine("\n  NODE STATES:");
               Console.WriteLine("  " + "Node ID".PadRight(12) + "Role".PadRight(12) + "Term".PadRight(8) + "CommitIdx".PadRight(12) + "LastApplied".PadRight(14) + "StoreCount".PadRight(12) + "Status");
               Console.WriteLine("  " + new string('-', 82));

               foreach (var handle in ClusterNodes.Values.OrderBy(h => h.NodeId))
               {
                  var roleStr = handle.Node.Role.ToString();
                  var termStr = handle.Node.CurrentTerm.ToString();
                  var commitStr = handle.Node.CommitIndex.ToString();
                  var appliedStr = handle.Node.LastApplied.ToString();
                  var storeCountStr = handle.StateMachine.Store.Count.ToString();

                  string statusStr;
                  if (handle.IsCrashed)
                     statusStr = "CRASHED";
                  else if (PartitionedNodes.ContainsKey(handle.NodeId))
                     statusStr = "PARTITIONED";
                  else
                     statusStr = "ONLINE";

                  Console.WriteLine("  " + handle.NodeId.PadRight(12) + roleStr.PadRight(12) + termStr.PadRight(8) + commitStr.PadRight(12) + appliedStr.PadRight(14) + storeCountStr.PadRight(12) + statusStr);
               }

               // Verify state consistency across active nodes
               var activeStores = ClusterNodes.Values
                  .Where(h => !h.IsCrashed && handleIsUpToDate(h))
                  .Select(h => h.StateMachine.Store)
                  .ToList();

               if (activeStores.Count > 1)
               {
                  var firstCount = activeStores[0].Count;
                  var isConsistent = activeStores.All(s => s.Count == firstCount);
                  if (isConsistent)
                  {
                     ConsoleRender.Success($"  State Consistency: OK (All online up-to-date nodes match {firstCount} state entries)");
                  }
               }
            }
         }
         catch
         {
            // Dashboard loop catch
         }
      }

      bool handleIsUpToDate(RaftNodeHandle h) => h.Node.Role is RaftRole.Leader or RaftRole.Follower;
   }

   private static int PromptInt(string label, int defaultValue)
   {
      Console.Write($"Enter {label} [default: {defaultValue}]: ");
      var input = Console.ReadLine();
      return int.TryParse(input, out var result) ? result : defaultValue;
   }

   private sealed class RaftNodeHandle(
      string nodeId,
      RaftNode node,
      IRaftLogStorage storage,
      ChaosStateMachine stateMachine,
      IRaftTransport transport,
      RaftNodeOptions options)
   {
      public string NodeId { get; } = nodeId;
      public RaftNode Node { get; } = node;
      public IRaftLogStorage Storage { get; } = storage;
      public ChaosStateMachine StateMachine { get; } = stateMachine;
      public IRaftTransport Transport { get; } = transport;
      public RaftNodeOptions Options { get; } = options;
      public bool IsCrashed { get; set; }
   }

   private sealed class ChaosStateMachine : IRaftStateMachine
   {
      public ConcurrentDictionary<string, string> Store { get; private set; } = new();

      public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
      {
         var text = Encoding.UTF8.GetString(command.Span);
         if (text.StartsWith("SET "))
         {
            var parts = text[4..].Split('=');
            if (parts.Length == 2)
            {
               Store[parts[0]] = parts[1];
            }
         }
         return ValueTask.FromResult<ReadOnlyMemory<byte>>("OK"u8.ToArray());
      }

      public ValueTask<ReadOnlyMemory<byte>> TakeSnapshotAsync(CancellationToken ct = default)
      {
         var dict = Store.ToDictionary(k => k.Key, v => v.Value);
         var json = JsonSerializer.SerializeToUtf8Bytes(dict);
         return ValueTask.FromResult<ReadOnlyMemory<byte>>(json);
      }

      public ValueTask RestoreSnapshotAsync(ReadOnlyMemory<byte> snapshot, ulong lastIncludedIndex, ulong lastIncludedTerm, CancellationToken ct = default)
      {
         if (!snapshot.IsEmpty)
         {
            var deserialized = JsonSerializer.Deserialize<Dictionary<string, string>>(snapshot.Span);
            if (deserialized != null)
            {
               Store = new ConcurrentDictionary<string, string>(deserialized);
            }
         }
         return ValueTask.CompletedTask;
      }
   }

   private sealed class PartitionableRaftTransport(
      INetworkListener listener,
      IEnumerable<RaftPeerEndpoint> peers,
      string localNodeId,
      ConcurrentDictionary<string, bool> partitionedNodes)
      : IRaftTransport
   {
      private readonly RaftNetworkTransport _inner = new(listener, peers);

      public ValueTask StartAsync(Func<RaftRpcRequest, ValueTask<RaftRpcResponse>> rpcHandler, CancellationToken ct = default)
      {
         return _inner.StartAsync(rpcHandler, ct);
      }

      public ValueTask StopAsync(CancellationToken ct = default)
      {
         return _inner.StopAsync(ct);
      }

      public async ValueTask<RequestVoteResponse?> RequestVoteAsync(string peerId, RequestVoteRequest request, CancellationToken ct = default)
      {
         if (partitionedNodes.ContainsKey(localNodeId) || partitionedNodes.ContainsKey(peerId))
         {
            return null; // Simulates network partition drop
         }

         return await _inner.RequestVoteAsync(peerId, request, ct);
      }

      public async ValueTask<AppendEntriesResponse?> AppendEntriesAsync(string peerId, AppendEntriesRequest request, CancellationToken ct = default)
      {
         if (partitionedNodes.ContainsKey(localNodeId) || partitionedNodes.ContainsKey(peerId))
         {
            return null; // Simulates network partition drop
         }

         return await _inner.AppendEntriesAsync(peerId, request, ct);
      }

      public async ValueTask<InstallSnapshotResponse?> InstallSnapshotAsync(string peerId, InstallSnapshotRequest request, CancellationToken ct = default)
      {
         if (partitionedNodes.ContainsKey(localNodeId) || partitionedNodes.ContainsKey(peerId))
         {
            return null; // Simulates network partition drop
         }

         return await _inner.InstallSnapshotAsync(peerId, request, ct);
      }

      public ValueTask DisposeAsync()
      {
         return _inner.DisposeAsync();
      }
   }
}
