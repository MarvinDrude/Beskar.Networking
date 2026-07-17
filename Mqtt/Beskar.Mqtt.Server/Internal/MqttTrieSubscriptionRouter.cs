using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server.Enumerators;
using Beskar.Networking.Abstractions.Comparers;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Mqtt.Server.Internal;

/// <summary>
/// High-performance, thread-safe, UTF-8 bytes-based Trie Subscription Router.
/// </summary>
public sealed class MqttTrieSubscriptionRouter : IDisposable
{
   private readonly MqttTrieNode _rootNode = new(null);
   private readonly ReadWriteLock _lock = new(LockRecursionPolicy.NoRecursion);

   private bool _disposed;

   public void Subscribe(
      MqttSession session,
      byte[] topicFilter,
      QualityOfServiceType qualityOfService,
      bool noLocal,
      bool retainAsPublished,
      RetainHandlingType retainHandling,
      uint subscriptionIdentifier)
   {
      using var disposer = _lock.EnterWriteLock();

      var enumerator = new TopicLevelEnumerator(topicFilter);
      var node = _rootNode;

      while (enumerator.MoveNext())
      {
         var level = enumerator.Current;
         if (level.SequenceEqual(PlusTagBytes))
         {
            node.SingleLevelWildcardChild ??= new MqttTrieNode(PlusTagBytes);
            node = node.SingleLevelWildcardChild;
         }
         else if (level.SequenceEqual(HashTagBytes))
         {
            node.MultiLevelWildcardChild ??= new MqttTrieNode(HashTagBytes);
            node = node.MultiLevelWildcardChild;
         }
         else
         {
            var children = node.Children;
            var lookup = children.GetAlternateLookup<ReadOnlySpan<byte>>();

            if (!lookup.TryGetValue(level, out var child))
            {
               var levelBytes = level.ToArray();
               child = new MqttTrieNode(levelBytes);

               children.Add(levelBytes, child);
            }

            node = child;
         }
      }

      var existing = node.Subscriptions.Find(s => s.Session == session);
      if (existing is not null)
      {
         existing.QualityOfService = qualityOfService;
         existing.NoLocal = noLocal;
         existing.RetainAsPublished = retainAsPublished;
         existing.RetainHandling = retainHandling;
         existing.SubscriptionIdentifier = subscriptionIdentifier;
      }
      else
      {
         node.Subscriptions.Add(new MqttSubscription(
            session,
            topicFilter,
            qualityOfService,
            noLocal,
            retainAsPublished,
            retainHandling,
            subscriptionIdentifier));
      }

      var options = new MqttSessionSubscription
      {
         QualityOfService = qualityOfService,
         NoLocal = noLocal,
         RetainAsPublished = retainAsPublished,
         RetainHandling = retainHandling,
         SubscriptionIdentifier = subscriptionIdentifier
      };

      session.Subscriptions[topicFilter] = options;
   }

   public bool Unsubscribe(MqttSession session, ReadOnlySpan<byte> topicFilter)
   {
      using var disposer = _lock.EnterWriteLock();

      var enumerator = new TopicLevelEnumerator(topicFilter);
      var removed = false;

      UnsubscribeRecursive(_rootNode, ref enumerator, session, ref removed);

      var alternateLookup = session.Subscriptions.GetAlternateLookup<ReadOnlySpan<byte>>();
      alternateLookup.Remove(topicFilter);

      return removed;
   }

   private static bool UnsubscribeRecursive(
      MqttTrieNode node,
      ref TopicLevelEnumerator levels,
      MqttSession session,
      ref bool removed)
   {
      if (!levels.MoveNext())
      {
         for (var i = 0; i < node.Subscriptions.Count; i++)
         {
            if (node.Subscriptions[i].Session != session)
               continue;

            node.Subscriptions.RemoveAt(i);
            removed = true;
            break;
         }

         return CheckNodeEmpty(node);
      }

      var currentLevel = levels.Current;
      if (currentLevel.SequenceEqual(PlusTagBytes))
      {
         if (node.SingleLevelWildcardChild is null)
            return CheckNodeEmpty(node);

         var nextLevels = levels;
         if (UnsubscribeRecursive(node.SingleLevelWildcardChild, ref nextLevels, session, ref removed))
         {
            node.SingleLevelWildcardChild = null;
         }
      }
      else if (currentLevel.SequenceEqual(HashTagBytes))
      {
         if (node.MultiLevelWildcardChild is null)
            return CheckNodeEmpty(node);

         var nextLevels = levels;
         if (UnsubscribeRecursive(node.MultiLevelWildcardChild, ref nextLevels, session, ref removed))
         {
            node.MultiLevelWildcardChild = null;
         }
      }
      else
      {
         var lookup = node.Children.GetAlternateLookup<ReadOnlySpan<byte>>();
         if (!lookup.TryGetValue(currentLevel, out var child))
            return CheckNodeEmpty(node);

         var nextLevels = levels;
         if (UnsubscribeRecursive(child, ref nextLevels, session, ref removed))
         {
            lookup.Remove(currentLevel);
         }
      }

      return CheckNodeEmpty(node);
   }

   private static bool CheckNodeEmpty(MqttTrieNode node)
   {
      return node.Level is not null
          && node.Subscriptions.Count == 0
          && node.Children.Count == 0
          && node.SingleLevelWildcardChild is null
          && node.MultiLevelWildcardChild is null;
   }

   public void UnsubscribeAll(MqttSession session)
   {
      using var disposer = _lock.EnterWriteLock();

      var count = session.Subscriptions.Count;
      if (count == 0) return;

      var filters = new byte[count][];
      var idx = 0;

      foreach (var key in session.Subscriptions.Keys)
      {
         filters[idx++] = key;
      }

      foreach (var filter in filters)
      {
         var enumerator = new TopicLevelEnumerator(filter);
         var dummy = false;
         UnsubscribeRecursive(_rootNode, ref enumerator, session, ref dummy);
      }

      session.Subscriptions.Clear();
   }

   public void Route<TVisitor>(ReadOnlySpan<byte> topic, ref TVisitor visitor) where TVisitor : struct, ISubscriptionVisitor
   {
      using var disposer = _lock.EnterReadLock();

      var enumerator = new TopicLevelEnumerator(topic);
      MatchRecursive(_rootNode, ref enumerator, ref visitor);
   }

   private static void MatchRecursive<TVisitor>(
      MqttTrieNode node,
      ref TopicLevelEnumerator levels,
      ref TVisitor visitor)
      where TVisitor : struct, ISubscriptionVisitor
   {
      if (node.MultiLevelWildcardChild is { Subscriptions: { } hashSubs })
      {
         for (var i = 0; i < hashSubs.Count; i++)
         {
            visitor.Visit(hashSubs[i]);
         }
      }

      if (!levels.MoveNext())
      {
         if (node.Subscriptions is not { } exactSubs) return;

         for (var i = 0; i < exactSubs.Count; i++)
         {
            visitor.Visit(exactSubs[i]);
         }

         return;
      }

      var currentLevel = levels.Current;

      var alternateLookup = node.Children.GetAlternateLookup<ReadOnlySpan<byte>>();
      if (alternateLookup.TryGetValue(currentLevel, out var exactChild))
      {
         var nextLevels = levels;
         MatchRecursive(exactChild, ref nextLevels, ref visitor);
      }

      if (node.SingleLevelWildcardChild is not null)
      {
         var nextLevels = levels;
         MatchRecursive(node.SingleLevelWildcardChild, ref nextLevels, ref visitor);
      }
   }

   public void Dispose()
   {
      if (_disposed) return;
      _disposed = true;

      _lock.Dispose();
   }

   private static readonly byte[] HashTagBytes = [.. "#"u8];
   private static readonly byte[] PlusTagBytes = [.. "+"u8];
}

/// <summary>
/// A visitor pattern interface to match subscriptions without allocating lists.
/// </summary>
public interface ISubscriptionVisitor
{
   void Visit(in MqttSubscription subscription);
}

/// <summary>
/// Represents a node in the UTF-8 bytes-based MQTT topic subscription trie.
/// </summary>
internal sealed class MqttTrieNode(byte[]? level)
{
   public byte[]? Level { get; } = level;

   public Dictionary<byte[], MqttTrieNode> Children =>
      field ??= new Dictionary<byte[], MqttTrieNode>(ByteArrayEqualityComparer.Instance);

   public MqttTrieNode? SingleLevelWildcardChild { get; set; }
   public MqttTrieNode? MultiLevelWildcardChild { get; set; }

   public List<MqttSubscription> Subscriptions => field ??= [];
}
