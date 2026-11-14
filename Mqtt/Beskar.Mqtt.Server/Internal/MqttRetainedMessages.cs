using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using Beskar.Mqtt.Common.Telemetry;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Server.Enumerators;
using Beskar.Networking.Abstractions.Comparers;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Mqtt.Server.Internal;

/// <summary>
/// High-performance, thread-safe manager for MQTT retained messages using a Trie structure.
/// </summary>
public sealed class MqttRetainedMessages : IDisposable
{
   private readonly MqttRetainedMessageNode _rootNode = new(null);
   private readonly ReadWriteLock _lock = new(LockRecursionPolicy.NoRecursion);

   private bool _disposed;

   public bool UpdateMessage(string clientId, MqttPublishMessage message)
   {
      using var disposer = _lock.EnterWriteLock();

      if (message.Payload.IsEmpty)
      {
         var enumerator = new TopicLevelEnumerator(Encoding.UTF8.GetBytes(message.Topic));
         RemoveMessageRecursive(_rootNode, ref enumerator, out var changed);
         if (changed)
         {
            MqttMetrics.RecordRetainedMessageChange(-1);
         }
         return changed;
      }
      else
      {
         var topicBytes = Encoding.UTF8.GetBytes(message.Topic);
         var enumerator = new TopicLevelEnumerator(topicBytes);
         var node = _rootNode;

         while (enumerator.MoveNext())
         {
            var level = enumerator.Current;
            var children = node.Children;
            var lookup = children.GetAlternateLookup<ReadOnlySpan<byte>>();

            if (!lookup.TryGetValue(level, out var child))
            {
               var levelBytes = level.ToArray();

               child = new MqttRetainedMessageNode(levelBytes);
               children.Add(levelBytes, child);
            }

            node = child;
         }

         var isNew = node.Message is null;
         var changed = isNew || !ReferenceEquals(node.Message, message);
         if (isNew)
         {
            MqttMetrics.RecordRetainedMessageChange(1);
         }
         node.Message = message;

         return changed;
      }
   }

   public void LoadMessages(IEnumerable<MqttPublishMessage> messages)
   {
      using var disposer = _lock.EnterWriteLock();

      foreach (var message in messages)
      {
         if (message.Payload.IsEmpty) continue;

         var topicBytes = Encoding.UTF8.GetBytes(message.Topic);
         var enumerator = new TopicLevelEnumerator(topicBytes);
         var node = _rootNode;

         while (enumerator.MoveNext())
         {
            var level = enumerator.Current;
            var children = node.Children;
            var lookup = children.GetAlternateLookup<ReadOnlySpan<byte>>();

            if (!lookup.TryGetValue(level, out var child))
            {
               var levelBytes = level.ToArray();

               child = new MqttRetainedMessageNode(levelBytes);
               children.Add(levelBytes, child);
            }

            node = child;
         }

         if (node.Message is null)
         {
            MqttMetrics.RecordRetainedMessageChange(1);
         }
         node.Message = message;
      }
   }

   public void GetMatchingMessages(ReadOnlySpan<byte> topicFilter, List<MqttPublishMessage> matched)
   {
      var utcNow = DateTimeOffset.UtcNow;
      var sawExpired = false;

      using (var disposer = _lock.EnterReadLock())
      {
         var enumerator = new TopicLevelEnumerator(topicFilter);
         MatchRecursive(_rootNode, ref enumerator, matched, utcNow, ref sawExpired, isFirstLevel: true);
      }

      if (sawExpired)
      {
         PruneExpired();
      }
   }

   private static void MatchRecursive(
      MqttRetainedMessageNode node,
      ref TopicLevelEnumerator levels,
      List<MqttPublishMessage> matched,
      DateTimeOffset utcNow,
      ref bool sawExpired,
      bool isFirstLevel = true)
   {
      if (!levels.MoveNext())
      {
         var msg = node.Message;

         if (msg is not null)
         {
            if (IsExpired(msg, utcNow))
            {
               sawExpired = true;
               return;
            }
            matched.Add(msg);
         }
         return;
      }

      var currentLevel = levels.Current;

      if (currentLevel.SequenceEqual(HashTagBytes))
      {
         // # matches this node and all descendants
         CollectAllRecursive(node, matched, utcNow, ref sawExpired, skipDollarChildren: isFirstLevel);
         return;
      }

      if (currentLevel.SequenceEqual(PlusTagBytes))
      {
         // + matches any level, so we must go down all child nodes
         foreach (var child in node.Children.Values)
         {
            if (isFirstLevel && child.Level is not null && child.Level.Length > 0 && child.Level[0] == (byte)'$')
            {
               continue;
            }
            var nextLevels = levels;
            MatchRecursive(child, ref nextLevels, matched, utcNow, ref sawExpired, isFirstLevel: false);
         }
         return;
      }

      var alternateLookup = node.Children.GetAlternateLookup<ReadOnlySpan<byte>>();
      if (alternateLookup.TryGetValue(currentLevel, out var exactChild))
      {
         var nextLevels = levels;
         MatchRecursive(exactChild, ref nextLevels, matched, utcNow, ref sawExpired, isFirstLevel: false);
      }
   }

   private static void CollectAllRecursive(
      MqttRetainedMessageNode node,
      List<MqttPublishMessage> matched,
      DateTimeOffset utcNow,
      ref bool sawExpired,
      bool skipDollarChildren = false)
   {
      var msg = node.Message;

      if (msg is not null)
      {
         if (IsExpired(msg, utcNow))
         {
            sawExpired = true;
         }
         else
         {
            matched.Add(msg);
         }
      }

      foreach (var child in node.Children.Values)
      {
         if (skipDollarChildren && child.Level is not null && child.Level.Length > 0 && child.Level[0] == (byte)'$')
         {
            continue;
         }
         CollectAllRecursive(child, matched, utcNow, ref sawExpired, skipDollarChildren: false);
      }
   }

   private static bool RemoveMessageRecursive(MqttRetainedMessageNode node, ref TopicLevelEnumerator levels, out bool changed)
   {
      if (!levels.MoveNext())
      {
         changed = node.Message is not null;
         node.Message = null;
         return CheckNodeEmpty(node);
      }

      var currentLevel = levels.Current;
      var children = node.Children;
      var lookup = children.GetAlternateLookup<ReadOnlySpan<byte>>();

      if (!lookup.TryGetValue(currentLevel, out var child))
      {
         changed = false;
         return CheckNodeEmpty(node);
      }

      var nextLevels = levels;
      if (RemoveMessageRecursive(child, ref nextLevels, out changed))
      {
         lookup.Remove(currentLevel);
      }

      return CheckNodeEmpty(node);
   }

   private static bool CheckNodeEmpty(MqttRetainedMessageNode node)
   {
      return node.Level is not null
          && node.Message is null
          && node.Children.Count == 0;
   }

   public List<MqttPublishMessage> GetMessages()
   {
      var utcNow = DateTimeOffset.UtcNow;
      var sawExpired = false;
      var result = new List<MqttPublishMessage>();

      using (var disposer = _lock.EnterReadLock())
      {
         CollectAllRecursive(_rootNode, result, utcNow, ref sawExpired);
      }

      if (sawExpired)
      {
         PruneExpired();
      }

      return result;
   }

   public void Clear()
   {
      using var disposer = _lock.EnterWriteLock();
      var count = CountAllMessagesRecursive(_rootNode);

      _rootNode.Children.Clear();
      _rootNode.Message = null;

      if (count > 0)
      {
         MqttMetrics.RecordRetainedMessageChange(-count);
      }
   }

   private void PruneExpired()
   {
      using var disposer = _lock.EnterWriteLock();
      var removed = PruneExpiredRecursive(_rootNode, DateTimeOffset.UtcNow);
      if (removed > 0)
      {
         MqttMetrics.RecordRetainedMessageChange(-removed);
      }
   }

   private static int PruneExpiredRecursive(MqttRetainedMessageNode node, DateTimeOffset utcNow)
   {
      var removed = 0;
      if (node.Message is not null && IsExpired(node.Message, utcNow))
      {
         node.Message = null;
         removed++;
      }

      if (node.Children.Count == 0)
      {
         return removed;
      }

      List<byte[]>? emptyChildren = null;
      foreach (var (level, child) in node.Children)
      {
         removed += PruneExpiredRecursive(child, utcNow);
         if (CheckNodeEmpty(child))
         {
            emptyChildren ??= [];
            emptyChildren.Add(level);
         }
      }

      if (emptyChildren is not null)
      {
         foreach (var level in emptyChildren)
         {
            node.Children.Remove(level);
         }
      }

      return removed;
   }

   private static int CountAllMessagesRecursive(MqttRetainedMessageNode node)
   {
      var count = node.Message is not null ? 1 : 0;
      foreach (var child in node.Children.Values)
      {
         count += CountAllMessagesRecursive(child);
      }

      return count;
   }

   private static bool IsExpired(MqttPublishMessage msg, DateTimeOffset utcNow)
   {
      if (msg.MessageExpiryInterval == 0)
      {
         return false;
      }

      var elapsed = utcNow - msg.CreatedAt;
      var timeSpent = elapsed > TimeSpan.Zero ? (uint)elapsed.TotalSeconds : 0u;
      return timeSpent >= msg.MessageExpiryInterval;
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

internal sealed class MqttRetainedMessageNode(byte[]? level)
{
   public byte[]? Level { get; } = level;

   public Dictionary<byte[], MqttRetainedMessageNode> Children =>
      field ??= new Dictionary<byte[], MqttRetainedMessageNode>(ByteArrayEqualityComparer.Instance);

   public MqttPublishMessage? Message { get; set; }
}
