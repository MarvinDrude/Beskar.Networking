using System.IO.Pipelines;
using Beskar.Memory.Code.PacketGenerator.Interfaces;
using Beskar.Memory.Extensions;
using Beskar.Memory.Serialization;
using Beskar.Memory.Writers;
using Beskar.Networking.Cluster.Protocol.Interfaces;
using Beskar.Networking.Cluster.Protocol.Packets;
using Beskar.Networking.Cluster.Protocol.Registries;

namespace Beskar.Networking.Cluster.Engine;

public sealed class ClusterCommunicator(
   Guid localNodeId,
   ClusterSessionRegistry sessionRegistry,
   ShardRoutingRegistry routingRegistry,
   ClusterMessageRegistry messageRegistry)
{
   private readonly ClusterSessionRegistry _sessionRegistry = sessionRegistry;
   private readonly ShardRoutingRegistry _routingRegistry = routingRegistry;
   private readonly ClusterMessageRegistry _messageRegistry = messageRegistry;

   private readonly Guid _localNodeId = localNodeId;
   private const ushort _version = 0x01;

   public Task BroadcastAsync<TPacket>(Guid shardId, scoped in TPacket payload,
      long currentEpoch, CancellationToken ct = default)
      where TPacket : IClusterPacketPayload, IPacket
   {
      var packet = new ClusterPacket<TPacket>()
      {
         Payload = payload,
         CorrelationId = Guid.CreateVersion7(),
         CurrentEpoch = currentEpoch,
         Version = _version,
         ShardId = shardId
      };

      using var targetNodes = _routingRegistry.RentReplicaNodes(shardId);
      using var arrayBuilder = new ArrayBuilder<Task>(targetNodes.Length);

      foreach (var nodeId in targetNodes)
      {
         if (nodeId == _localNodeId)
            continue;

         arrayBuilder.Add(SendAsync(nodeId, packet, ct));
      }

      return Awaited(arrayBuilder);

      static async Task Awaited(ArrayBuilder<Task> tasks)
      {
         await Task.WhenAll(tasks.WrittenSpan)
            .WithAggregateException();
      }
   }

   public Task SendAsync<TPacket>(Guid targetNodeId,
      scoped in ClusterPacket<TPacket> packet, CancellationToken ct = default)
      where TPacket : IClusterPacketPayload, IPacket
   {
      if (!_sessionRegistry.TryGetSession(targetNodeId, out var stream))
      {
         return Task.CompletedTask;
      }

      // length + packet header length
      var requiredLength = BeSerializer.CalculateByteLength(packet) + sizeof(int);

      var memory = stream.Transport.Output.GetMemory(requiredLength);
      var writer = new BufferWriter<byte>(memory.Span);

      try
      {
         _messageRegistry.SerializeWithHeader(ref writer, packet);
         stream.Transport.Output.Advance(requiredLength);
      }
      finally
      {
         // should never allocate a buffer itself
         // - since we are precalc exact size and rent it
         writer.Dispose();
      }

      return Awaited(stream.Transport.Output, ct);

      static async Task Awaited(PipeWriter pipe, CancellationToken ctt)
      {
         await pipe.FlushAsync(ctt);
      }
   }

   public Task SendAsync<TPacket>(Guid shardId, Guid targetNodeId, scoped in TPacket payload,
      long currentEpoch, CancellationToken ct = default)
      where TPacket : IClusterPacketPayload, IPacket
   {
      var packet = new ClusterPacket<TPacket>()
      {
         Payload = payload,
         CorrelationId = Guid.CreateVersion7(),
         CurrentEpoch = currentEpoch,
         Version = _version,
         ShardId = shardId
      };

      return SendAsync(targetNodeId, packet, ct);
   }
}
