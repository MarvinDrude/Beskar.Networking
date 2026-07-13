
using System.Diagnostics.CodeAnalysis;
using Beskar.Mqtt.Common.Generators;
using Beskar.Networking.Abstractions.Comparers;

namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttSession : IAsyncDisposable
{
   public DateTimeOffset? DisconnectionTimestamp { get; internal set; }

   public Dictionary<byte[], MqttSessionSubscription> Subscriptions { get; } = new(ByteArrayEqualityComparer.Instance);

   private readonly HashSet<ushort> _incomingQos2Packets = [];
   private readonly PacketIdentifierGenerator _packetIdGenerator = new();
   private readonly Queue<MqttQueuedMessage> _offlineQueue = new();

   public ushort GenerateNextPacketIdentifier() => _packetIdGenerator.GenerateNextIdentifier();

   public bool TryAddQos2Packet(ushort packetIdentifier)
   {
      lock (_incomingQos2Packets)
      {
         return _incomingQos2Packets.Add(packetIdentifier);
      }
   }

   public void RemoveQos2Packet(ushort packetIdentifier)
   {
      lock (_incomingQos2Packets)
      {
         _incomingQos2Packets.Remove(packetIdentifier);
      }
   }

   public int OfflineQueueCount
   {
      get
      {
         lock (_offlineQueue)
         {
            return _offlineQueue.Count;
         }
      }
   }

   internal void EnqueueOfflineMessage(MqttQueuedMessage message)
   {
      lock (_offlineQueue)
      {
         _offlineQueue.Enqueue(message);
      }
   }

   internal bool TryDequeueOfflineMessage([NotNullWhen(true)] out MqttQueuedMessage? message)
   {
      lock (_offlineQueue)
      {
         return _offlineQueue.TryDequeue(out message);
      }
   }
}
