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

   public Dictionary<byte[], MqttSessionSubscription> Subscriptions { get; }
      = new(ByteArrayEqualityComparer.Instance);

   private readonly object _subscriptionsLock = new();

   public bool HasSubscription(ReadOnlySpan<byte> topicFilter)
   {
      lock (_subscriptionsLock)
      {
         var alternateLookup = Subscriptions.GetAlternateLookup<ReadOnlySpan<byte>>();
         return alternateLookup.ContainsKey(topicFilter);
      }
   }

   public void AddOrUpdateSubscription(byte[] topicFilter, MqttSessionSubscription subscription)
   {
      lock (_subscriptionsLock)
      {
         Subscriptions[topicFilter] = subscription;
      }
   }

   public bool RemoveSubscription(ReadOnlySpan<byte> topicFilter)
   {
      lock (_subscriptionsLock)
      {
         var alternateLookup = Subscriptions.GetAlternateLookup<ReadOnlySpan<byte>>();
         return alternateLookup.Remove(topicFilter);
      }
   }

   public int GetSubscriptionsCount()
   {
      lock (_subscriptionsLock)
      {
         return Subscriptions.Count;
      }
   }

   public List<byte[]> GetSubscriptionKeys()
   {
      lock (_subscriptionsLock)
      {
         return [.. Subscriptions.Keys];
      }
   }

   public void ClearSubscriptions()
   {
      lock (_subscriptionsLock)
      {
         Subscriptions.Clear();
      }
   }

   private readonly HashSet<ushort> _incomingQos2Packets = [];
   private readonly PacketIdentifierGenerator _packetIdGenerator = new();
   private readonly Queue<MqttQueuedMessage> _offlineQueue = new();
   private readonly List<MqttPendingPublish> _unacknowledgedPublishes = [];

   public bool HasUnacknowledgedPublishes
   {
      get
      {
         lock (_unacknowledgedPublishes)
         {
            return _unacknowledgedPublishes.Count > 0;
         }
      }
   }

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

   internal void AddUnacknowledgedPublish(MqttPendingPublish pendingPublish)
   {
      lock (_unacknowledgedPublishes)
      {
         _unacknowledgedPublishes.Add(pendingPublish);
      }
   }

   internal MqttPendingPublish? AcknowledgePublish(ushort packetIdentifier)
   {
      lock (_unacknowledgedPublishes)
      {
         var found = _unacknowledgedPublishes.Find(p => p.PacketIdentifier == packetIdentifier);
         if (found is not null)
         {
            _unacknowledgedPublishes.Remove(found);
         }

         return found;
      }
   }

   internal MqttPendingPublish? PeekUnacknowledgedPublish(ushort packetIdentifier)
   {
      lock (_unacknowledgedPublishes)
      {
         return _unacknowledgedPublishes.Find(p => p.PacketIdentifier == packetIdentifier);
      }
   }

   internal int GetUnacknowledgedPublishCount()
   {
      lock (_unacknowledgedPublishes)
      {
         return _unacknowledgedPublishes.Count;
      }
   }

   internal List<MqttPendingPublish> GetUnacknowledgedPublishes()
   {
      lock (_unacknowledgedPublishes)
      {
         return [.. _unacknowledgedPublishes];
      }
   }

   public ushort ClientReceiveMaximum { get; internal set; } = 65535;

   private int _incomingInFlightCount;

   public bool TryIncrementIncomingInFlight(ushort receiveMaximum, out int current)
   {
      lock (_incomingQos2Packets)
      {
         current = _incomingInFlightCount;
         if (receiveMaximum > 0 && current >= receiveMaximum)
         {
            return false;
         }

         _incomingInFlightCount++;
         return true;
      }
   }

   public void DecrementIncomingInFlight()
   {
      lock (_incomingQos2Packets)
      {
         if (_incomingInFlightCount > 0)
         {
            _incomingInFlightCount--;
         }
      }
   }
}
