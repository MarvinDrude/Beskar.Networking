using System.Text;
using Beskar.Mqtt.Common.Matching;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttTopicMatcherTests
{
   [Test]
   public async Task ExactMatch_ReturnsTrue()
   {
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/player1", "sport/tennis/player1")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/player1"u8, "sport/tennis/player1"u8)).IsTrue();
   }

   [Test]
   public async Task ExactMismatch_ReturnsFalse()
   {
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/player1", "sport/tennis/player2")).IsFalse();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/player1"u8, "sport/tennis/player2"u8)).IsFalse();
   }

   [Test]
   public async Task SingleLevelWildcard_Middle_ReturnsTrue()
   {
      await Assert.That(MqttTopicMatcher.IsMatch("sport/+/player1", "sport/tennis/player1")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/+/player1", "sport/football/player1")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/+/player1", "sport/tennis/player2")).IsFalse();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/+/player1", "sport/tennis/foo/player1")).IsFalse();

      await Assert.That(MqttTopicMatcher.IsMatch("sport/+/player1"u8, "sport/tennis/player1"u8)).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/+/player1"u8, "sport/tennis/foo/player1"u8)).IsFalse();
   }

   [Test]
   public async Task SingleLevelWildcard_End_ReturnsTrue()
   {
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/+", "sport/tennis/player1")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/+", "sport/tennis/player1/ranking")).IsFalse();

      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/+"u8, "sport/tennis/player1"u8)).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/+"u8, "sport/tennis/player1/ranking"u8)).IsFalse();
   }

   [Test]
   public async Task SingleLevelWildcard_Start_ReturnsTrue()
   {
      await Assert.That(MqttTopicMatcher.IsMatch("+/tennis/player1", "sport/tennis/player1")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("+/tennis/player1", "news/tennis/player1")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("+/tennis/player1", "sports/football/player1")).IsFalse();

      await Assert.That(MqttTopicMatcher.IsMatch("+/tennis/player1"u8, "sport/tennis/player1"u8)).IsTrue();
   }

   [Test]
   public async Task MultiLevelWildcard_MatchesEverythingUnderneath()
   {
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/#", "sport/tennis/player1")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/#", "sport/tennis/player1/ranking")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/#", "sport/tennis/player1/score/wimbledon")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/#", "sport/football")).IsFalse();

      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/#"u8, "sport/tennis/player1"u8)).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/#"u8, "sport/tennis/player1/ranking"u8)).IsTrue();
   }

   [Test]
   public async Task MultiLevelWildcard_MatchesParentTopic()
   {
      // MQTT specification 4.7.1.2: "sport/tennis/#" matches "sport/tennis"
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/#", "sport/tennis")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/tennis/#"u8, "sport/tennis"u8)).IsTrue();
   }

   [Test]
   public async Task MultiLevelWildcard_RootHash_MatchesAnyNormalTopic()
   {
      await Assert.That(MqttTopicMatcher.IsMatch("#", "sport/tennis/player1")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("#", "finance")).IsTrue();

      await Assert.That(MqttTopicMatcher.IsMatch("#"u8, "sport/tennis/player1"u8)).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("#"u8, "finance"u8)).IsTrue();
   }

   [Test]
   public async Task SystemTopicDollar_CannotBeMatchedByRootWildcard()
   {
      // MQTT specification: leading $ cannot be matched by + or # at the first level
      await Assert.That(MqttTopicMatcher.IsMatch("#", "$SYS/broker/uptime")).IsFalse();
      await Assert.That(MqttTopicMatcher.IsMatch("+/broker/uptime", "$SYS/broker/uptime")).IsFalse();

      await Assert.That(MqttTopicMatcher.IsMatch("#"u8, "$SYS/broker/uptime"u8)).IsFalse();
      await Assert.That(MqttTopicMatcher.IsMatch("+/broker/uptime"u8, "$SYS/broker/uptime"u8)).IsFalse();

      // But explicit $SYS matches
      await Assert.That(MqttTopicMatcher.IsMatch("$SYS/#", "$SYS/broker/uptime")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("$SYS/+/uptime", "$SYS/broker/uptime")).IsTrue();

      await Assert.That(MqttTopicMatcher.IsMatch("$SYS/#"u8, "$SYS/broker/uptime"u8)).IsTrue();
   }

   [Test]
   public async Task EmptyLevels_HandledPerSpec()
   {
      // "sport/+" matches "sport/"
      await Assert.That(MqttTopicMatcher.IsMatch("sport/+", "sport/")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("sport/+"u8, "sport/"u8)).IsTrue();

      // "+/+" matches "/finance"
      await Assert.That(MqttTopicMatcher.IsMatch("+/+", "/finance")).IsTrue();
      await Assert.That(MqttTopicMatcher.IsMatch("+/+"u8, "/finance"u8)).IsTrue();
   }
}
