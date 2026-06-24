using System.Buffers;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Common.Tests.Builders;

public class SubscribeOptionsTests
{
   [Test]
   public async Task CorrectOptionsBuildingAndSerialization()
   {
      // Arrange
      var builder = new SubscribeOptionsBuilder()
         .WithSubscriptionIdentifier(42)
         .WithTopicFilter("sports/tennis", QualityOfServiceType.AtLeastOnce, noLocal: true, retainAsPublished: false, RetainHandlingType.SendOnNewSubscriptionOnly)
         .WithTopicFilter("sports/golf", QualityOfServiceType.ExactlyOnce, noLocal: false, retainAsPublished: true, RetainHandlingType.DoNotSend)
         .WithUserProperty("custom-key", "custom-value");

      // Act
      var options = builder.Build();

      // Extract values synchronously to avoid holding ref structs across await boundaries
      var subscriptionId = options.SubscriptionIdentifier;

      var hasFilter1 = false;
      var filter1Name = "";
      var filter1Qos = QualityOfServiceType.AtMostOnce;
      var filter1NoLocal = false;
      var filter1RetainAsPublished = false;
      var filter1RetainHandling = RetainHandlingType.SendAtSubscription;

      var hasFilter2 = false;
      var filter2Name = "";
      var filter2Qos = QualityOfServiceType.AtMostOnce;
      var filter2NoLocal = false;
      var filter2RetainAsPublished = false;
      var filter2RetainHandling = RetainHandlingType.SendAtSubscription;

      var hasMoreFilters = true;

      {
         var enumerator = options.TopicFilters.GetEnumerator();
         if (enumerator.MoveNext())
         {
            hasFilter1 = true;
            var filter1 = enumerator.Current;
            var bytes1 = new byte[filter1.TopicUtf8Bytes.Length];
            filter1.TopicUtf8Bytes.CopyTo(bytes1);
            filter1Name = System.Text.Encoding.UTF8.GetString(bytes1);
            filter1Qos = filter1.QualityOfService;
            filter1NoLocal = filter1.NoLocal;
            filter1RetainAsPublished = filter1.RetainAsPublished;
            filter1RetainHandling = filter1.RetainHandling;

            if (enumerator.MoveNext())
            {
               hasFilter2 = true;
               var filter2 = enumerator.Current;
               var bytes2 = new byte[filter2.TopicUtf8Bytes.Length];
               filter2.TopicUtf8Bytes.CopyTo(bytes2);
               filter2Name = System.Text.Encoding.UTF8.GetString(bytes2);
               filter2Qos = filter2.QualityOfService;
               filter2NoLocal = filter2.NoLocal;
               filter2RetainAsPublished = filter2.RetainAsPublished;
               filter2RetainHandling = filter2.RetainHandling;

               hasMoreFilters = enumerator.MoveNext();
            }
         }
      }

      var hasUserProp = false;
      var userPropKey = "";
      var userPropVal = "";
      var hasMoreUserProps = true;

      {
         var userPropEnum = options.UserProperties.GetEnumerator();
         if (userPropEnum.MoveNext())
         {
            hasUserProp = true;
            var userProp = userPropEnum.Current;
            userPropKey = System.Text.Encoding.UTF8.GetString(userProp.KeyUtf8Bytes);
            userPropVal = System.Text.Encoding.UTF8.GetString(userProp.ValueBytes);
            hasMoreUserProps = userPropEnum.MoveNext();
         }
      }

      // Assert at the end (after all ref structs are out of scope)
      await Assert.That(subscriptionId).IsEqualTo(42U);

      await Assert.That(hasFilter1).IsTrue();
      await Assert.That(filter1Name).IsEqualTo("sports/tennis");
      await Assert.That(filter1Qos).IsEqualTo(QualityOfServiceType.AtLeastOnce);
      await Assert.That(filter1NoLocal).IsTrue();
      await Assert.That(filter1RetainAsPublished).IsFalse();
      await Assert.That(filter1RetainHandling).IsEqualTo(RetainHandlingType.SendOnNewSubscriptionOnly);

      await Assert.That(hasFilter2).IsTrue();
      await Assert.That(filter2Name).IsEqualTo("sports/golf");
      await Assert.That(filter2Qos).IsEqualTo(QualityOfServiceType.ExactlyOnce);
      await Assert.That(filter2NoLocal).IsFalse();
      await Assert.That(filter2RetainAsPublished).IsTrue();
      await Assert.That(filter2RetainHandling).IsEqualTo(RetainHandlingType.DoNotSend);

      await Assert.That(hasMoreFilters).IsFalse();

      await Assert.That(hasUserProp).IsTrue();
      await Assert.That(userPropKey).IsEqualTo("custom-key");
      await Assert.That(userPropVal).IsEqualTo("custom-value");
      await Assert.That(hasMoreUserProps).IsFalse();
   }

   [Test]
   public async Task ClearingOptionsResetsState()
   {
      // Arrange
      var builder = new SubscribeOptionsBuilder()
         .WithSubscriptionIdentifier(100)
         .WithTopicFilter("temp", QualityOfServiceType.AtMostOnce)
         .WithUserProperty("k", "v");

      var options = builder.Build();

      // Act
      options.Clear();

      // Assert
      await Assert.That(options.SubscriptionIdentifier).IsEqualTo(0U);
      await Assert.That(options.TopicFilters.Count).IsEqualTo(0);
      await Assert.That(options.UserProperties.Count).IsEqualTo(0);
   }
}
