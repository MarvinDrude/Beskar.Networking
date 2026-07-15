using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Text;
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

      var changed = false;
      if (message.Payload.IsEmpty)
      {
         if (node.Message is null) return changed;

         node.Message = null;
      }
      else
      {
         node.Message = message;
      }

      changed = true;
      return changed;
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

         node.Message = message;
      }
   }

   public void GetMatchingMessages(ReadOnlySpan<byte> topicFilter, List<MqttPublishMessage> matched)
   {
      using var disposer = _lock.EnterReadLock();

      var enumerator = new TopicLevelEnumerator(topicFilter);
      MatchRecursive(_rootNode, ref enumerator, matched);
   }

   private static void MatchRecursive(
      MqttRetainedMessageNode node,
      ref TopicLevelEnumerator levels,
      List<MqttPublishMessage> matched)
   {
      if (!levels.MoveNext())
      {
         if (node.Message is not null)
         {
            if (node.Message.MessageExpiryInterval > 0)
            {
               var timeSpent = (uint)(DateTimeOffset.UtcNow - node.Message.CreatedAt).TotalSeconds;
               if (timeSpent >= node.Message.MessageExpiryInterval)
               {
                  node.Message = null;
                  return;
               }
            }
            matched.Add(node.Message);
         }
         return;
      }

      var currentLevel = levels.Current;

      if (currentLevel.SequenceEqual(HashTagBytes))
      {
         // # matches this node and all descendants
         CollectAllRecursive(node, matched);
         return;
      }

      if (currentLevel.SequenceEqual(PlusTagBytes))
      {
         // + matches any level, so we must go down all child nodes
         foreach (var child in node.Children.Values)
         {
            var nextLevels = levels;
            MatchRecursive(child, ref nextLevels, matched);
         }
         return;
      }

      var alternateLookup = node.Children.GetAlternateLookup<ReadOnlySpan<byte>>();
      if (alternateLookup.TryGetValue(currentLevel, out var exactChild))
      {
         var nextLevels = levels;
         MatchRecursive(exactChild, ref nextLevels, matched);
      }
   }

   private static void CollectAllRecursive(MqttRetainedMessageNode node, List<MqttPublishMessage> matched)
   {
      if (node.Message is not null)
      {
         if (node.Message.MessageExpiryInterval > 0)
         {
            var timeSpent = (uint)(DateTimeOffset.UtcNow - node.Message.CreatedAt).TotalSeconds;
            if (timeSpent >= node.Message.MessageExpiryInterval)
            {
               node.Message = null;
            }
            else
            {
               matched.Add(node.Message);
            }
         }
         else
         {
            matched.Add(node.Message);
         }
      }

      foreach (var child in node.Children.Values)
      {
         CollectAllRecursive(child, matched);
      }
   }

   public List<MqttPublishMessage> GetMessages()
   {
      using var disposer = _lock.EnterReadLock();
      var result = new List<MqttPublishMessage>();
      CollectAllRecursive(_rootNode, result);

      return result;
   }

   public void Clear()
   {
      using var disposer = _lock.EnterWriteLock();
      _rootNode.Children.Clear();
      _rootNode.Message = null;
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
