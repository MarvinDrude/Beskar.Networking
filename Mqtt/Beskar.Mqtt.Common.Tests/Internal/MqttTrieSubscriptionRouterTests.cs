using System.Buffers;
using System.Text;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Enumerators;
using Beskar.Mqtt.Server.Internal;
using Beskar.Mqtt.Server.Options;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Common.Tests.Internal;

public class MqttTrieSubscriptionRouterTests
{
   [Test]
   public async Task TopicLevelEnumerator_ShouldEnumerateCorrectly()
   {
      // Test cases
      await AssertEnumerator("a/b/c", ["a", "b", "c"]);
      await AssertEnumerator("a/+/c", ["a", "+", "c"]);
      await AssertEnumerator("a/#", ["a", "#"]);
      await AssertEnumerator("#", ["#"]);
      await AssertEnumerator("/", ["", ""]);
      await AssertEnumerator("/a", ["", "a"]);
      await AssertEnumerator("a/", ["a", ""]);
      await AssertEnumerator("a//b", ["a", "", "b"]);
   }

   private static async Task AssertEnumerator(string topic, string[] expectedLevels)
   {
      var bytes = Encoding.UTF8.GetBytes(topic);
      var enumerator = new TopicLevelEnumerator(bytes);
      var list = new List<string>();

      while (enumerator.MoveNext()) list.Add(Encoding.UTF8.GetString(enumerator.Current));

      await Assert.That(list).Count().IsEqualTo(expectedLevels.Length);
      for (var i = 0; i < expectedLevels.Length; i++) await Assert.That(list[i]).IsEqualTo(expectedLevels[i]);
   }

   [Test]
   public async Task Route_ExactMatch_ShouldMatchCorrectly()
   {
      using var router = new MqttTrieSubscriptionRouter();
      await using var server = new MqttServer(Array.Empty<INetworkListener>(), new MqttServerOptions());
      var session1 = new MqttSession(server, null!);

      router.Subscribe(session1, "a/b/c"u8.ToArray(), QualityOfServiceType.AtLeastOnce, false, false,
         RetainHandlingType.SendAtSubscription, 0);

      var visitor = new TestVisitor();
      router.Route("a/b/c"u8, ref visitor);

      await Assert.That(visitor.Matches).Count().IsEqualTo(1);
      await Assert.That(visitor.Matches[0].Session).IsSameReferenceAs(session1);
      await Assert.That(Encoding.UTF8.GetString(visitor.Matches[0].TopicFilter)).IsEqualTo("a/b/c");

      // Non-matching query
      var visitor2 = new TestVisitor();
      router.Route("a/b/d"u8, ref visitor2);
      await Assert.That(visitor2.Matches).IsEmpty();
   }

   [Test]
   public async Task Route_SingleLevelWildcard_ShouldMatchCorrectly()
   {
      using var router = new MqttTrieSubscriptionRouter();
      await using var server = new MqttServer(Array.Empty<INetworkListener>(), new MqttServerOptions());
      var session1 = new MqttSession(server, null!);

      router.Subscribe(session1, "a/+/c"u8.ToArray(), QualityOfServiceType.AtLeastOnce, false, false,
         RetainHandlingType.SendAtSubscription, 0);

      // Matches
      var visitor1 = new TestVisitor();
      router.Route("a/b/c"u8, ref visitor1);
      await Assert.That(visitor1.Matches).Count().IsEqualTo(1);
      await Assert.That(visitor1.Matches[0].Session).IsSameReferenceAs(session1);

      var visitor2 = new TestVisitor();
      router.Route("a/foo/c"u8, ref visitor2);
      await Assert.That(visitor2.Matches).Count().IsEqualTo(1);

      // Non-matches
      var visitor3 = new TestVisitor();
      router.Route("a/b/d"u8, ref visitor3);
      await Assert.That(visitor3.Matches).IsEmpty();

      var visitor4 = new TestVisitor();
      router.Route("a/b/c/d"u8, ref visitor4);
      await Assert.That(visitor4.Matches).IsEmpty();
   }

   [Test]
   public async Task Route_MultiLevelWildcard_ShouldMatchCorrectly()
   {
      using var router = new MqttTrieSubscriptionRouter();
      await using var server = new MqttServer(Array.Empty<INetworkListener>(), new MqttServerOptions());
      var session1 = new MqttSession(server, null!);

      router.Subscribe(session1, "a/#"u8.ToArray(), QualityOfServiceType.AtLeastOnce, false, false,
         RetainHandlingType.SendAtSubscription, 0);

      // Matches
      var visitor1 = new TestVisitor();
      router.Route("a"u8, ref visitor1);
      await Assert.That(visitor1.Matches).Count().IsEqualTo(1);

      var visitor2 = new TestVisitor();
      router.Route("a/b"u8, ref visitor2);
      await Assert.That(visitor2.Matches).Count().IsEqualTo(1);

      var visitor3 = new TestVisitor();
      router.Route("a/b/c/d"u8, ref visitor3);
      await Assert.That(visitor3.Matches).Count().IsEqualTo(1);

      // Non-matches
      var visitor4 = new TestVisitor();
      router.Route("b/c"u8, ref visitor4);
      await Assert.That(visitor4.Matches).IsEmpty();
   }

   [Test]
   public async Task Route_Unsubscribe_ShouldRemoveMatchingSubscription()
   {
      using var router = new MqttTrieSubscriptionRouter();
      await using var server = new MqttServer(Array.Empty<INetworkListener>(), new MqttServerOptions());
      var session1 = new MqttSession(server, null!);

      var filter = "a/b/c"u8.ToArray();
      router.Subscribe(session1, filter, QualityOfServiceType.AtLeastOnce, false, false,
         RetainHandlingType.SendAtSubscription, 0);

      var visitor1 = new TestVisitor();
      router.Route("a/b/c"u8, ref visitor1);
      await Assert.That(visitor1.Matches).Count().IsEqualTo(1);

      // Unsubscribe
      router.Unsubscribe(session1, filter);

      var visitor2 = new TestVisitor();
      router.Route("a/b/c"u8, ref visitor2);
      await Assert.That(visitor2.Matches).IsEmpty();
   }

   [Test]
   public async Task Route_UnsubscribeAll_ShouldRemoveAllSubscriptionsForSession()
   {
      using var router = new MqttTrieSubscriptionRouter();
      await using var server = new MqttServer([], new MqttServerOptions());
      var session1 = new MqttSession(server, null!);

      router.Subscribe(session1, [.. "a/b/c"u8], QualityOfServiceType.AtLeastOnce, false, false,
         RetainHandlingType.SendAtSubscription, 0);
      router.Subscribe(session1, [.. "x/+/y"u8], QualityOfServiceType.ExactlyOnce, false, false,
         RetainHandlingType.SendAtSubscription, 0);

      var visitor1 = new TestVisitor();
      router.Route("a/b/c"u8, ref visitor1);
      await Assert.That(visitor1.Matches).Count().IsEqualTo(1);

      var visitor2 = new TestVisitor();
      router.Route("x/foo/y"u8, ref visitor2);
      await Assert.That(visitor2.Matches).Count().IsEqualTo(1);

      // Unsubscribe all
      router.UnsubscribeAll(session1);

      var visitor3 = new TestVisitor();
      router.Route("a/b/c"u8, ref visitor3);
      await Assert.That(visitor3.Matches).IsEmpty();

      var visitor4 = new TestVisitor();
      router.Route("x/foo/y"u8, ref visitor4);
      await Assert.That(visitor4.Matches).IsEmpty();
   }

   [Test]
   public async Task Server_UnsubscribeSpanAndSequence_ShouldWorkAndBeAllocationFree()
   {
      await using var server = new MqttServer([], new MqttServerOptions());
      var session1 = new MqttSession(server, null!);

      var filter = "a/b/c"u8.ToArray();
      server.Subscribe(session1, new TopicFilter(new ReadOnlySequence<byte>(filter), QualityOfServiceType.AtLeastOnce));

      // 1. Unsubscribe using ReadOnlySpan<byte>
      var filterSpan = new ReadOnlySpan<byte>(filter);

      // Warmup/Resolve TUnit lazy initialization if any
      server.Unsubscribe(session1, filterSpan);
      server.Subscribe(session1, new TopicFilter(new ReadOnlySequence<byte>(filter), QualityOfServiceType.AtLeastOnce));

      server.Unsubscribe(session1, filterSpan);

      // Verify unsubscribed
      var visitor1 = new TestVisitor();
      server.SubscriptionRouter.Route("a/b/c"u8, ref visitor1);
      await Assert.That(visitor1.Matches).IsEmpty();

      // 2. Unsubscribe using ReadOnlySequence<byte>
      server.Subscribe(session1, new TopicFilter(new ReadOnlySequence<byte>(filter), QualityOfServiceType.AtLeastOnce));
      var filterSeq = new ReadOnlySequence<byte>(filter);

      server.Unsubscribe(session1, filterSeq);

      // Verify unsubscribed
      var visitor2 = new TestVisitor();
      server.SubscriptionRouter.Route("a/b/c"u8, ref visitor2);
      await Assert.That(visitor2.Matches).IsEmpty();
   }

   private struct TestVisitor : ISubscriptionVisitor
   {
      public List<MqttSubscription> Matches { get; } = new();

      public TestVisitor()
      {
      }

      public void Visit(in MqttSubscription subscription)
      {
         Matches.Add(subscription);
      }
   }

   private struct EmptyVisitor : ISubscriptionVisitor
   {
      public int VisitCount;

      public void Visit(in MqttSubscription subscription)
      {
         VisitCount++;
      }
   }
}
