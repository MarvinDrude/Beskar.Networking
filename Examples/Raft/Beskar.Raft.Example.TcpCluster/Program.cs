using System.Net;
using System.Text;
using Beskar.Networking.Raft;
using Beskar.Networking.Raft.Enums;
using Beskar.Networking.Raft.Options;
using Beskar.Networking.Raft.StateMachine;
using Beskar.Networking.Raft.Storage;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Tcp;

Console.WriteLine("=== Beskar Raft TCP Multi-Node Cluster Example ===");

var basePort = 11050;
var nodeConfigs = new[]
{
   ("tcp-node-1", new IPEndPoint(IPAddress.Loopback, basePort + 1)),
   ("tcp-node-2", new IPEndPoint(IPAddress.Loopback, basePort + 2)),
   ("tcp-node-3", new IPEndPoint(IPAddress.Loopback, basePort + 3)),
};

var tcpOptions = new TcpTransportOptions();
var nodes = new List<RaftNode>();

for (var i = 0; i < nodeConfigs.Length; i++)
{
   var (id, endpoint) = nodeConfigs[i];
   var peerConfigs = nodeConfigs.Where(c => c.Item1 != id).ToList();

   var peerEndpoints = peerConfigs.Select(p => new RaftPeerEndpoint(
      p.Item1,
      p.Item2,
      () => new TcpNetworkClient(tcpOptions))).ToList();

   var listener = new TcpNetworkListener(endpoint, tcpOptions);
   var transport = new RaftNetworkTransport(listener, peerEndpoints, TimeSpan.FromMilliseconds(200));

   var options = new RaftNodeOptions
   {
      NodeId = id,
      Peers = peerConfigs.Select(p => p.Item1).ToList(),
      ElectionTimeoutMin = TimeSpan.FromMilliseconds(150),
      ElectionTimeoutMax = TimeSpan.FromMilliseconds(300),
      HeartbeatInterval = TimeSpan.FromMilliseconds(50)
   };

   var storage = new InMemoryRaftLogStorage();
   var sm = new TcpClusterStateMachine(id);

   var node = new RaftNode(options, storage, sm, transport);
   nodes.Add(node);
}

// 1. Start all 3 nodes over TCP
Console.WriteLine("Starting 3 Raft nodes over TCP sockets...");
foreach (var node in nodes)
{
   await node.StartAsync();
}

// 2. Wait for a leader to be elected over TCP RPCs
RaftNode? leader = null;
var deadline = Environment.TickCount64 + 4000;
while (Environment.TickCount64 < deadline)
{
   leader = nodes.FirstOrDefault(n => n.Role == RaftRole.Leader);
   if (leader != null) break;
   await Task.Delay(50);
}

if (leader == null)
{
   Console.WriteLine("Failed to elect leader in time.");
   return;
}

Console.WriteLine($"\n[Cluster] Leader elected over TCP: '{leader.Options.NodeId}' in Term {leader.CurrentTerm}");

// 3. Propose replicated commands through TCP cluster leader
var command1 = Encoding.UTF8.GetBytes("ORDER_PLACED:order-9001");
Console.WriteLine($"\n[Client] Proposing command '{Encoding.UTF8.GetString(command1)}' to leader...");
var resp1 = await leader.ProposeAsync(command1);
Console.WriteLine($"[Client] Quorum commit response: {Encoding.UTF8.GetString(resp1.Span)}");

var command2 = Encoding.UTF8.GetBytes("ORDER_SHIPPED:order-9001");
Console.WriteLine($"[Client] Proposing command '{Encoding.UTF8.GetString(command2)}' to leader...");
var resp2 = await leader.ProposeAsync(command2);
Console.WriteLine($"[Client] Quorum commit response: {Encoding.UTF8.GetString(resp2.Span)}");

await Task.Delay(200);

// 4. Graceful teardown
Console.WriteLine("\nShutting down TCP cluster...");
foreach (var node in nodes)
{
   await node.DisposeAsync();
}

Console.WriteLine("Done!");

internal sealed class TcpClusterStateMachine(string nodeId) : IRaftStateMachine
{
   private readonly string _nodeId = nodeId;

   public ValueTask<ReadOnlyMemory<byte>> ApplyAsync(
      ReadOnlyMemory<byte> command, ulong logIndex, CancellationToken ct = default)
   {
      var str = Encoding.UTF8.GetString(command.Span);
      Console.WriteLine($"  [{_nodeId}] Applied committed log #{logIndex}: '{str}'");

      var response = Encoding.UTF8.GetBytes($"ACK_TCP:{str}");
      return ValueTask.FromResult<ReadOnlyMemory<byte>>(response);
   }
}
