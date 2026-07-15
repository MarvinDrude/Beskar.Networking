using System.Buffers;
using System.Text;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Mqtt.Server.Internal;
using Beskar.Mqtt.Server.Options;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttRetainedMessagesTests
{
   [Test]
   public async Task UpdateMessage_ShouldStoreAndPruneCorrectly()
   {
      using var manager = new MqttRetainedMessages();

      var topic = "sensor/temp/1";
      var payload = "22.5"u8.ToArray();
      var msg = CreatePublishMessage(topic, payload);

      // Store message
      var changed = manager.UpdateMessage("client1", msg);
      await Assert.That(changed).IsTrue();

      var messages = manager.GetMessages();
      await Assert.That(messages).Count().IsEqualTo(1);
      await Assert.That(messages[0].Topic).IsEqualTo(topic);
      await Assert.That(messages[0].Payload.ToArray()).IsEquivalentTo(payload);

      // Prune message with empty payload
      var emptyMsg = CreatePublishMessage(topic, Array.Empty<byte>());
      var changed2 = manager.UpdateMessage("client1", emptyMsg);
      await Assert.That(changed2).IsTrue();

      var messages2 = manager.GetMessages();
      await Assert.That(messages2).IsEmpty();
   }

   [Test]
   public async Task GetMatchingMessages_ShouldMatchWildcardsCorrectly()
   {
      using var manager = new MqttRetainedMessages();

      manager.UpdateMessage("c1", CreatePublishMessage("a/b/c", "1"u8.ToArray()));
      manager.UpdateMessage("c1", CreatePublishMessage("a/x/c", "2"u8.ToArray()));
      manager.UpdateMessage("c1", CreatePublishMessage("a/b/d", "3"u8.ToArray()));
      manager.UpdateMessage("c1", CreatePublishMessage("foo/bar", "4"u8.ToArray()));

      // Exact match
      var matchedExact = new List<MqttPublishMessage>();
      manager.GetMatchingMessages("a/b/c"u8, matchedExact);
      await Assert.That(matchedExact).Count().IsEqualTo(1);
      await Assert.That(matchedExact[0].Topic).IsEqualTo("a/b/c");

      // Single-level wildcard match
      var matchedPlus = new List<MqttPublishMessage>();
      manager.GetMatchingMessages("a/+/c"u8, matchedPlus);
      await Assert.That(matchedPlus).Count().IsEqualTo(2);
      var topicsPlus = matchedPlus.Select(m => m.Topic).ToList();
      await Assert.That(topicsPlus).Contains("a/b/c");
      await Assert.That(topicsPlus).Contains("a/x/c");

      // Multi-level wildcard match
      var matchedHash = new List<MqttPublishMessage>();
      manager.GetMatchingMessages("a/#"u8, matchedHash);
      await Assert.That(matchedHash).Count().IsEqualTo(3);
      var topicsHash = matchedHash.Select(m => m.Topic).ToList();
      await Assert.That(topicsHash).Contains("a/b/c");
      await Assert.That(topicsHash).Contains("a/x/c");
      await Assert.That(topicsHash).Contains("a/b/d");
   }

   [Test]
   public async Task Server_LoadingAndClearingRetainedMessagesEvents_ShouldWork()
   {
      await using var server = new MqttServer([], new MqttServerOptions());

      var loadedTriggered = false;
      server.Events.OnLoadingRetainedMessages.Add((context, ct) =>
      {
         loadedTriggered = true;
         context.LoadedRetainedMessages.Add(CreatePublishMessage("loaded/topic", "hello"u8.ToArray()));
         return ValueTask.CompletedTask;
      });

      var clearedTriggered = false;
      server.Events.OnRetainedMessagesCleared.Add((context, ct) =>
      {
         clearedTriggered = true;
         return ValueTask.CompletedTask;
      });

      // Start triggers OnLoadingRetainedMessages
      var startResult = await server.StartAsync();
      await Assert.That(startResult.Failed).IsFalse();
      await Assert.That(loadedTriggered).IsTrue();

      var stored = server.RetainedMessages.GetMessages();
      await Assert.That(stored).Count().IsEqualTo(1);
      await Assert.That(stored[0].Topic).IsEqualTo("loaded/topic");

      // Clear triggers OnRetainedMessagesCleared
      await server.ClearRetainedMessagesAsync();
      await Assert.That(clearedTriggered).IsTrue();
      await Assert.That(server.RetainedMessages.GetMessages()).IsEmpty();
   }

   [Test]
   public async Task UpdateMessage_WithExpiredInterval_ShouldNotBeDeliveredAndShouldBePruned()
   {
      using var manager = new MqttRetainedMessages();

      var topic = "sensor/temp/1";
      var payload = "22.5"u8.ToArray();
      
      // Create message with 1 second expiry
      var msg = new MqttPublishMessage(new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = true,
         TopicUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(topic)),
         Payload = new ReadOnlySequence<byte>(payload),
         MessageExpiryInterval = 1,
      });

      // Store message
      var changed = manager.UpdateMessage("client1", msg);
      await Assert.That(changed).IsTrue();

      // It should be there immediately
      var messages = manager.GetMessages();
      await Assert.That(messages).Count().IsEqualTo(1);

      // Now wait 1.5 seconds for it to expire
      await Task.Delay(1500);

      // Checking messages now should filter it out and prune it
      var messagesAfterExpiry = manager.GetMessages();
      await Assert.That(messagesAfterExpiry).IsEmpty();

      // Verify it's actually removed from the trie
      var matched = new List<MqttPublishMessage>();
      manager.GetMatchingMessages("sensor/temp/1"u8, matched);
      await Assert.That(matched).IsEmpty();
   }

   private static MqttPublishMessage CreatePublishMessage(string topic, byte[] payload)
   {
      return new MqttPublishMessage(new PublishPacket
      {
         Dup = false,
         QualityOfService = QualityOfServiceType.AtMostOnce,
         Retain = true,
         TopicUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(topic)),
         Payload = new ReadOnlySequence<byte>(payload),
         PacketIdentifier = 0,
         PayloadFormat = PayloadFormat.Unspecified,
         MessageExpiryInterval = 0,
         TopicAlias = 0,
         ResponseTopicUtf8Bytes = ReadOnlySequence<byte>.Empty,
         CorrelationDataBytes = ReadOnlySequence<byte>.Empty,
         ContentTypeUtf8Bytes = ReadOnlySequence<byte>.Empty,
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      });
   }
}
