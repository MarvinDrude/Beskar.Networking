using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Utilities.Console.Rendering;

namespace Beskar.Mqtt.Example.NodeRegistry;

/// <summary>
/// Watches the MQTT node registry (<c>registry/nodes/+</c>), keeps a live in-memory view
/// of all currently online nodes, and provides query/listing methods.
/// </summary>
public sealed class NodeRegistryWatcher(IPEndPoint brokerEndPoint, string watcherId, bool logEvents = true)
   : IAsyncDisposable
{
   private readonly IMqttClient _client = MqttClientFactory.CreateTcp();
   private readonly ConcurrentDictionary<string, NodeRegistration> _nodes = new(StringComparer.Ordinal);
   private readonly SemaphoreSlim _signal = new(0);
   private IAsyncDisposable? _subscriptionToken;
   private bool _disposed;

   public int Count => _nodes.Count;

   public async Task StartAsync(CancellationToken ct = default)
   {
      var connectOptions = new ConnectOptions
      {
         EndPoint = brokerEndPoint,
         ProtocolVersion = MqttProtocolVersion.V50,
         ClientIdUtf8Bytes = Encoding.UTF8.GetBytes(watcherId),
         CleanSession = true
      };

      var connectResult = await _client.ConnectAsync(connectOptions, ct);
      if (connectResult.Failed)
      {
         throw new InvalidOperationException($"Watcher '{watcherId}' failed to connect: {connectResult.Error.Detail}");
      }

      _subscriptionToken = await _client.SubscribeTopicAsync(
         NodeRegistryTopics.AllNodesFilter,
         (context, _) =>
         {
            if (!NodeRegistryTopics.TryExtractNodeId(context.Message.Topic, out var nodeId))
            {
               return ValueTask.CompletedTask;
            }

            if (context.Message.Payload.IsEmpty)
            {
               if (_nodes.TryRemove(nodeId, out var removed) && logEvents)
               {
                  Console.WriteLine($"    [Watcher:{watcherId}] REMOVED node '{removed.NodeId}' (role={removed.Role}, retained={context.Message.Retain})");
               }
            }
            else
            {
               var registration = JsonSerializer.Deserialize<NodeRegistration>(
                  context.Message.Payload.Span,
                  NodeRegistration.JsonOptions);

               if (registration is not null)
               {
                  var isNew = !_nodes.ContainsKey(nodeId);
                  _nodes[nodeId] = registration;

                  if (logEvents)
                  {
                     var verb = isNew ? "REGISTERED" : "UPDATED";
                     Console.WriteLine($"    [Watcher:{watcherId}] {verb} node '{registration.NodeId}' ({registration.Role} @ {registration.Endpoint}, load={registration.ActiveLoadPercent}%)");
                  }
               }
            }

            _signal.Release();
            return ValueTask.CompletedTask;
         },
         QualityOfServiceType.AtLeastOnce,
         ct);
   }

   /// <summary>
   /// Returns all currently registered nodes sorted by <see cref="NodeRegistration.NodeId"/>.
   /// </summary>
   public IReadOnlyList<NodeRegistration> ListNodes()
      => [.. _nodes.Values.OrderBy(n => n.NodeId, StringComparer.Ordinal)];

   /// <summary>
   /// Returns all currently registered nodes matching the specified role.
   /// </summary>
   public IReadOnlyList<NodeRegistration> FindByRole(string role)
      => [.. _nodes.Values
         .Where(n => string.Equals(n.Role, role, StringComparison.OrdinalIgnoreCase))
         .OrderBy(n => n.NodeId, StringComparer.Ordinal)];

   /// <summary>
   /// Looks up a single node by its identifier.
   /// </summary>
   public bool TryGetNode(string nodeId, out NodeRegistration? registration)
      => _nodes.TryGetValue(nodeId, out registration);

   /// <summary>
   /// Waits until the registry reaches the expected node count (or times out).
   /// </summary>
   public async Task WaitForNodeCountAsync(int expectedCount, TimeSpan timeout, CancellationToken ct = default)
   {
      var deadline = DateTime.UtcNow + timeout;
      while (_nodes.Count != expectedCount)
      {
         var remaining = deadline - DateTime.UtcNow;
         if (remaining <= TimeSpan.Zero)
         {
            throw new TimeoutException(
               $"Watcher '{watcherId}' timed out waiting for {expectedCount} nodes (actual: {_nodes.Count}).");
         }

         await _signal.WaitAsync(remaining, ct);
      }
   }

   /// <summary>
   /// Renders a formatted table of all currently registered nodes to the console.
   /// </summary>
   public void PrintNodeList(string heading)
   {
      var nodes = ListNodes();
      Console.WriteLine();
      Console.WriteLine($"  {heading} ({nodes.Count} active)");

      if (nodes.Count == 0)
      {
         Console.WriteLine("    (no nodes currently registered)");
         return;
      }

      var table = ConsoleRender.CreateTable()
         .AddColumn("Node ID", Alignment.Left, ConsoleColor.Cyan)
         .AddColumn("Role", Alignment.Left, ConsoleColor.Yellow)
         .AddColumn("Endpoint", Alignment.Left, ConsoleColor.White)
         .AddColumn("Version", Alignment.Center, ConsoleColor.Gray)
         .AddColumn("Load", Alignment.Right, ConsoleColor.Green)
         .AddColumn("Capabilities", Alignment.Left, ConsoleColor.Magenta);

      foreach (var node in nodes)
      {
         table.AddRow(
            node.NodeId,
            node.Role,
            node.Endpoint,
            node.Version,
            $"{node.ActiveLoadPercent}%",
            string.Join(", ", node.Capabilities));
      }

      table.Render();
   }

   public async ValueTask DisposeAsync()
   {
      if (_disposed) return;
      _disposed = true;

      if (_subscriptionToken is not null)
      {
         await _subscriptionToken.DisposeAsync();
      }

      if (_client.IsConnected)
      {
         await _client.DisconnectAsync(new DisconnectOptions());
      }

      await _client.DisposeAsync();
      _signal.Dispose();
   }
}
