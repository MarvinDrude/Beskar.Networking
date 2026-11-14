using System.Net;
using Beskar.Mqtt.Example.NodeRegistry;
using Beskar.Mqtt.Server;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = false;

Console.WriteLine();
Console.WriteLine("=================================================================");
Console.WriteLine(" MQTT Node Registry (Retained Announcements + Last Will Cleanup) ");
Console.WriteLine("=================================================================");

const int Port = 8013;
var brokerEndPoint = new IPEndPoint(IPAddress.Loopback, Port);
var timeout = TimeSpan.FromSeconds(5);

// 1. Start the embedded MQTT v5.0 Broker
Console.WriteLine("\n[1] Starting MQTT Broker on port {0}...", Port);
var mqttServer = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(Port)
   .Build();

var startResult = await mqttServer.StartAsync();
if (startResult.Failed)
{
   throw new InvalidOperationException($"Broker failed to start: {startResult.Error.Detail}");
}
Console.WriteLine("    Broker is listening.");

// 2. Start a live NodeRegistryWatcher subscribed to 'registry/nodes/+'
Console.WriteLine("\n[2] Starting live NodeRegistryWatcher...");
await using var liveWatcher = new NodeRegistryWatcher(brokerEndPoint, "registry-watcher-live");
await liveWatcher.StartAsync();
Console.WriteLine("    Watcher subscribed to '{0}'.", NodeRegistryTopics.AllNodesFilter);

// 3. Connect and announce cluster nodes, each with a retained empty-payload Last Will
Console.WriteLine("\n[3] Announcing 4 nodes (each configured with a retained empty-payload LWT)...");
await using var apiGateway = new RegistryNodeClient(
   brokerEndPoint, "api-gateway-01", "gateway", "10.0.1.10:8080", "2.4.0", "http", "tls", "rate-limit");

await using var workerMl = new RegistryNodeClient(
   brokerEndPoint, "worker-ml-01", "worker", "10.0.2.21:9001", "2.4.0", "gpu", "inference", "onnx");

await using var workerEtl = new RegistryNodeClient(
   brokerEndPoint, "worker-etl-02", "worker", "10.0.2.22:9002", "2.4.0", "cpu", "parquet", "ingest");

await using var cacheNode = new RegistryNodeClient(
   brokerEndPoint, "cache-redis-01", "cache", "10.0.3.15:6379", "7.2.4", "kv", "pubsub");

await apiGateway.AnnounceAsync();
await workerMl.AnnounceAsync();
await workerEtl.AnnounceAsync();
await cacheNode.AnnounceAsync();

await liveWatcher.WaitForNodeCountAsync(4, timeout);

// 4. List all registered nodes and demonstrate queries
Console.WriteLine("\n[4] Listing and querying registered nodes...");
liveWatcher.PrintNodeList("Cluster Node Registry (Initial)");

var workers = liveWatcher.FindByRole("worker");
Console.WriteLine("    Query FindByRole(\"worker\"): {0}", string.Join(", ", workers.Select(w => w.NodeId)));

if (liveWatcher.TryGetNode("api-gateway-01", out var gw) && gw is not null)
{
   Console.WriteLine("    Query TryGetNode(\"api-gateway-01\"): Endpoint={0}, Capabilities=[{1}]",
      gw.Endpoint, string.Join(", ", gw.Capabilities));
}

// 5. Update a node's status in-place (retained payload overwrite)
Console.WriteLine("\n[5] Updating load metric for 'worker-etl-02'...");
await workerEtl.UpdateStatusAsync(activeLoadPercent: 68);
await Task.Delay(150);

// 6. Simulate an abrupt crash of 'worker-ml-01' (no DISCONNECT packet sent)
//    The broker fires its Last Will (retained empty payload on 'registry/nodes/worker-ml-01'),
//    automatically deleting the retained record and notifying live watchers.
Console.WriteLine("\n[6] Simulating crash of 'worker-ml-01' (abrupt socket close without DISCONNECT)...");
await workerMl.SimulateCrashAsync();

await liveWatcher.WaitForNodeCountAsync(3, timeout);
liveWatcher.PrintNodeList("Cluster Node Registry (After 'worker-ml-01' Crash)");

// 7. Connect a brand-new late-joining inspector client to prove the broker purged the crashed node
Console.WriteLine("\n[7] Connecting a late-joining Inspector client to verify retained state on broker...");
await using (var lateInspector = new NodeRegistryWatcher(brokerEndPoint, "registry-inspector-late", logEvents: false))
{
   await lateInspector.StartAsync();
   await lateInspector.WaitForNodeCountAsync(3, timeout);
   lateInspector.PrintNodeList("Late-Joining Inspector Snapshot (from Broker Retained Store)");
}

// 8. Gracefully deregister 'cache-redis-01' (clears retained topic + sends DISCONNECT, LWT discarded)
Console.WriteLine("\n[8] Gracefully deregistering 'cache-redis-01'...");
await cacheNode.DeregisterAndDisconnectAsync();

await liveWatcher.WaitForNodeCountAsync(2, timeout);
liveWatcher.PrintNodeList("Cluster Node Registry (After Graceful Deregistration)");

// 9. Recover / re-announce 'worker-ml-01'
Console.WriteLine("\n[9] Restarting and re-announcing 'worker-ml-01'...");
await using var recoveredWorkerMl = new RegistryNodeClient(
   brokerEndPoint, "worker-ml-01", "worker", "10.0.2.21:9001", "2.4.1", "gpu", "inference", "onnx");
await recoveredWorkerMl.AnnounceAsync();

await liveWatcher.WaitForNodeCountAsync(3, timeout);
liveWatcher.PrintNodeList("Cluster Node Registry (After 'worker-ml-01' Recovery)");

// 10. Clean shutdown
Console.WriteLine("\n[10] Shutting down remaining nodes, watcher, and broker...");
await apiGateway.DeregisterAndDisconnectAsync();
await workerEtl.DeregisterAndDisconnectAsync();
await recoveredWorkerMl.DeregisterAndDisconnectAsync();
await liveWatcher.DisposeAsync();
await mqttServer.StopAsync();
await mqttServer.DisposeAsync();

Console.WriteLine();
Console.WriteLine("=================================================================");
Console.WriteLine(" MQTT Node Registry Example Completed Successfully!              ");
Console.WriteLine("=================================================================");
