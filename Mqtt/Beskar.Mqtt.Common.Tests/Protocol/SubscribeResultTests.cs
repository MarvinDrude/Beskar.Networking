using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Results;

namespace Beskar.Mqtt.Common.Tests.Protocol;

public class SubscribeResultTests
{
   [Test]
   public async Task SubscribeResult_CanBeConstructedAndRead()
   {
      // Arrange
      var topicFilter = new MqttTopicFilter(new TopicFilter(
         new System.Buffers.ReadOnlySequence<byte>([.. "test/topic"u8]),
         QualityOfServiceType.AtLeastOnce,
         noLocal: true,
         retainAsPublished: false,
         RetainHandlingType.SendOnNewSubscriptionOnly
      ));

      var subscriptionResult = new MqttTopicSubscriptionResult
      {
         TopicFilter = topicFilter,
         ReasonCode = SubscribeReasonCode.GrantedQos1
      };

      var subscriptions = new[] { subscriptionResult };

      // Act
      var result = new SubscribeResult
      {
         PacketIdentifier = 123,
         ReasonString = "Success",
         Subscriptions = subscriptions,
         UserProperties = null! // not tested here
      };

      // Assert
      await Assert.That(result.PacketIdentifier).IsEqualTo((ushort)123);
      await Assert.That(result.ReasonString).IsEqualTo("Success");
      await Assert.That(result.Subscriptions).Count().IsEqualTo(1);
      await Assert.That(result.Subscriptions[0].TopicFilter.Topic).IsEqualTo("test/topic");
      await Assert.That(result.Subscriptions[0].TopicFilter.QualityOfService).IsEqualTo(QualityOfServiceType.AtLeastOnce);
      await Assert.That(result.Subscriptions[0].TopicFilter.NoLocal).IsTrue();
      await Assert.That(result.Subscriptions[0].ReasonCode).IsEqualTo(SubscribeReasonCode.GrantedQos1);
   }
}
