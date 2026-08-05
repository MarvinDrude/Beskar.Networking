using System.Diagnostics.CodeAnalysis;
using Beskar.Mqtt.Common.Generators;
using Beskar.Mqtt.Server.Enums;
using Beskar.Networking.Abstractions.Comparers;

namespace Beskar.Mqtt.Server.Internal;

/// <summary>
/// Represents a session for an MQTT client connection on the server side.
/// Encapsulates state data, such as subscriptions, unacknowledged messages,
/// and offline message queue related to the session.
/// </summary>
public sealed partial class MqttSession : IAsyncDisposable
{
   /// <summary>
   /// Gets the timestamp at which the client session was disconnected.
   /// </summary>
   /// <remarks>
   /// The <c>DisconnectionTimestamp</c> property represents the exact time when the session transitioned
   /// to an offline state. If the session is still connected, this property will be <c>null</c>.
   /// This property is primarily used for session management, expiring offline sessions, and cleaning
   /// up resources in accordance with the configured session expiry interval.
   /// </remarks>
   public DateTimeOffset? DisconnectionTimestamp { get; internal set; }

   /// <summary>
   /// Indicates whether the session has expired based on its disconnection timestamp and expiry interval.
   /// </summary>
   /// <remarks>
   /// The <c>IsExpired</c> property evaluates to <c>true</c> if the session's disconnection timestamp is set
   /// and enough time has elapsed based on the configured <c>ExpiryInterval</c>. This determination is made
   /// by comparing the timestamp, adjusted by the expiry interval, against the current UTC time.
   /// A session with an <c>ExpiryInterval</c> set to <c>uint.MaxValue</c> will never be marked as expired.
   /// This property is used to manage session lifecycle events and resource cleanup in scenarios
   /// such as offline message queuing.
   /// </remarks>
   public bool IsExpired => DisconnectionTimestamp is { } timestamp
       && ExpiryInterval != uint.MaxValue
       && timestamp.AddSeconds(ExpiryInterval) <= DateTimeOffset.UtcNow;

   internal MqttServer Server { get; }
   internal SemaphoreSlim DeliverySemaphore { get; } = new(1, 1);

   private Dictionary<byte[], MqttSessionSubscription> Subscriptions { get; }
      = new(ByteArrayEqualityComparer.Instance);

   private readonly Lock _subscriptionsLock = new();
   private readonly Lock _incomingQos2PacketsLock = new();
   private readonly Lock _offlineQueueLock = new();
   private readonly Lock _unacknowledgedPublishesLock = new();
   private int _incomingInFlightCount;

   private readonly HashSet<ushort> _incomingQos2Packets = [];
   private readonly PacketIdentifierGenerator _packetIdGenerator = new();
   private readonly Queue<MqttQueuedMessage> _offlineQueue = new();
   private readonly List<MqttPendingPublish> _unacknowledgedPublishes = [];

   internal bool HasUnacknowledgedPublishes
   {
      get
      {
         lock (_unacknowledgedPublishesLock)
         {
            return _unacknowledgedPublishes.Count > 0;
         }
      }
   }

   internal bool HasSubscription(ReadOnlySpan<byte> topicFilter)
   {
      lock (_subscriptionsLock)
      {
         var alternateLookup = Subscriptions.GetAlternateLookup<ReadOnlySpan<byte>>();
         return alternateLookup.ContainsKey(topicFilter);
      }
   }

   internal void AddOrUpdateSubscription(byte[] topicFilter, MqttSessionSubscription subscription)
   {
      lock (_subscriptionsLock)
      {
         Subscriptions[topicFilter] = subscription;
      }
   }

   internal bool RemoveSubscription(ReadOnlySpan<byte> topicFilter)
   {
      lock (_subscriptionsLock)
      {
         var alternateLookup = Subscriptions.GetAlternateLookup<ReadOnlySpan<byte>>();
         return alternateLookup.Remove(topicFilter);
      }
   }

   internal int GetSubscriptionsCount()
   {
      lock (_subscriptionsLock)
      {
         return Subscriptions.Count;
      }
   }

   internal List<byte[]> GetSubscriptionKeys()
   {
      lock (_subscriptionsLock)
      {
         return [.. Subscriptions.Keys];
      }
   }

   internal void ClearSubscriptions()
   {
      lock (_subscriptionsLock)
      {
         Subscriptions.Clear();
      }
   }

   internal ushort GenerateNextPacketIdentifier() => _packetIdGenerator.GenerateNextIdentifier();

   internal bool TryAddQos2Packet(ushort packetIdentifier)
   {
      lock (_incomingQos2PacketsLock)
      {
         return _incomingQos2Packets.Add(packetIdentifier);
      }
   }

   internal void RemoveQos2Packet(ushort packetIdentifier)
   {
      lock (_incomingQos2PacketsLock)
      {
         _incomingQos2Packets.Remove(packetIdentifier);
      }
   }

   internal int OfflineQueueCount
   {
      get
      {
         lock (_offlineQueueLock)
         {
            return _offlineQueue.Count;
         }
      }
   }

   internal void EnqueueOfflineMessage(MqttQueuedMessage message)
   {
      lock (_offlineQueueLock)
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
      lock (_offlineQueueLock)
      {
         return _offlineQueue.TryDequeue(out message);
      }
   }

   internal void AddUnacknowledgedPublish(MqttPendingPublish pendingPublish)
   {
      lock (_unacknowledgedPublishesLock)
      {
         _unacknowledgedPublishes.Add(pendingPublish);
      }
   }

   internal MqttPendingPublish? AcknowledgePublish(ushort packetIdentifier)
   {
      lock (_unacknowledgedPublishesLock)
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
      lock (_unacknowledgedPublishesLock)
      {
         return _unacknowledgedPublishes.Find(p => p.PacketIdentifier == packetIdentifier);
      }
   }

   internal int GetUnacknowledgedPublishCount()
   {
      lock (_unacknowledgedPublishesLock)
      {
         return _unacknowledgedPublishes.Count;
      }
   }

   internal List<MqttPendingPublish> GetUnacknowledgedPublishes()
   {
      lock (_unacknowledgedPublishesLock)
      {
         return [.. _unacknowledgedPublishes];
      }
   }

   internal bool TryIncrementIncomingInFlight(ushort receiveMaximum, out int current)
   {
      lock (_incomingQos2PacketsLock)
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

   internal void DecrementIncomingInFlight()
   {
      lock (_incomingQos2PacketsLock)
      {
         if (_incomingInFlightCount > 0)
         {
            _incomingInFlightCount--;
         }
      }
   }

   internal void ResetIncomingInFlight()
   {
      lock (_incomingQos2PacketsLock)
      {
         _incomingInFlightCount = 0;
      }
   }
}
