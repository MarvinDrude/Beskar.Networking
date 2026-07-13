using System.Diagnostics.CodeAnalysis;
using Beskar.Mqtt.Common.Generators;
using Beskar.Mqtt.Server.Enums;
using Beskar.Networking.Abstractions.Comparers;

namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttSession : IAsyncDisposable
{
   public DateTimeOffset? DisconnectionTimestamp { get; internal set; }

   public bool IsExpired => DisconnectionTimestamp is { } timestamp
       && ExpiryInterval != uint.MaxValue
       && timestamp.AddSeconds(ExpiryInterval) <= DateTimeOffset.UtcNow;

   internal MqttServer Server { get; }

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
         var max = Server.Options.MaxPendingMessagesPerConnection;
         if (max > 0 && _offlineQueue.Count >= max)
         {
            // if behavior is DropNewest, we drop the incoming message
            if (Server.Options.PendingMessageOverflowBehavior is not MessageOverflowBehavior.DropOldest)
               return;

            _offlineQueue.TryDequeue(out _);
         }

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
