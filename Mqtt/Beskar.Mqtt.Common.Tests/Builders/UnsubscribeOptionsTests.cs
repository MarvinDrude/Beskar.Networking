using System.Buffers;
using System.Threading.Tasks;
using Beskar.Mqtt.Common.Builders.Unsubscribing;

namespace Beskar.Mqtt.Common.Tests.Builders;

public class UnsubscribeOptionsTests
{
   [Test]
   public async Task CorrectOptionsBuildingAndSerialization()
   {
      // Arrange
      var builder = new UnsubscribeOptionsBuilder()
         .WithTopicFilter("sports/tennis")
         .WithTopicFilter("sports/golf")
         .WithUserProperty("custom-key", "custom-value");

      // Act
      var options = builder.Build();

      // Extract values
      var hasFilter1 = false;
      var filter1Name = "";

      var hasFilter2 = false;
      var filter2Name = "";

      var hasMoreFilters = true;

      {
         var enumerator = options.TopicFilters.GetEnumerator();
         if (enumerator.MoveNext())
         {
            hasFilter1 = true;
            filter1Name = System.Text.Encoding.UTF8.GetString(enumerator.Current);

            if (enumerator.MoveNext())
            {
               hasFilter2 = true;
               filter2Name = System.Text.Encoding.UTF8.GetString(enumerator.Current);

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
      await Assert.That(hasFilter1).IsTrue();
      await Assert.That(filter1Name).IsEqualTo("sports/tennis");

      await Assert.That(hasFilter2).IsTrue();
      await Assert.That(filter2Name).IsEqualTo("sports/golf");

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
      var builder = new UnsubscribeOptionsBuilder()
         .WithTopicFilter("temp")
         .WithUserProperty("k", "v");

      var options = builder.Build();

      // Act
      options.Clear();

      // Assert
      await Assert.That(options.TopicFilters.Count).IsEqualTo(0);
      await Assert.That(options.UserProperties.Count).IsEqualTo(0);
   }

   [Test]
   public async Task ValidationSuccessAndFailure()
   {
      // Valid topic filters
      var validOptions = new UnsubscribeOptionsBuilder()
         .WithTopicFilter("sports/tennis")
         .WithTopicFilter("sports/golf/#")
         .Build();
      var validResult = UnsubscribeOptionsValidator.Validate(validOptions);
      await Assert.That(validResult.IsSuccess).IsTrue();

      // Invalid topic filters (e.g. empty or wildcards in the wrong place)
      var invalidOptions1 = new UnsubscribeOptionsBuilder()
         .WithTopicFilter("")
         .Build();
      var invalidResult1 = UnsubscribeOptionsValidator.Validate(invalidOptions1);
      await Assert.That(invalidResult1.IsSuccess).IsFalse();
      await Assert.That(invalidResult1.Error.Detail).IsEqualTo("Topic should not be empty.");

      var invalidOptions2 = new UnsubscribeOptionsBuilder()
         .WithTopicFilter("sports/#/tennis")
         .Build();
      var invalidResult2 = UnsubscribeOptionsValidator.Validate(invalidOptions2);
      await Assert.That(invalidResult2.IsSuccess).IsFalse();
      await Assert.That(invalidResult2.Error.Detail).IsEqualTo("The character '#' is only allowed at the end of the topic.");
   }
}
