using System.Buffers;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Protocol.Enumerators;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;

namespace Beskar.Mqtt.Common.Tests.Builders;

public class PublishOptionsTests
{
   [Test]
   public async Task CorrectPublishOptionsBuildingAndSerialization()
   {
      // Arrange
      var builder = new PublishOptionsBuilder()
         .WithDup(true)
         .WithQualityOfService(QualityOfServiceType.ExactlyOnce)
         .WithRetain(true)
         .WithTopic("sensor/temp"u8)
         .WithPayload("hello-world")
         .WithPayloadFormat(PayloadFormat.CharacterData)
         .WithMessageExpiryInterval(3600)
         .WithTopicAlias(5)
         .WithResponseTopic("sensor/temp/response"u8)
         .WithCorrelationData([1, 2, 3])
         .WithContentType("text/plain")
         .WithUserProperty("region", "EU");

      // Act
      var options = builder.Build();

      // Assert normal properties
      var dup = options.Dup;
      var qos = options.QualityOfService;
      var retain = options.Retain;

      byte[] topicBytes = options.TopicUtf8Bytes.ToArray();
      var topicStr = System.Text.Encoding.UTF8.GetString(topicBytes);

      byte[] payloadBytes = options.Payload.ToArray();
      var payloadStr = System.Text.Encoding.UTF8.GetString(payloadBytes);

      // Verify serialized properties (ref struct scope block)
      bool hasPayloadFormat = false;
      PayloadFormat payloadFormatVal = PayloadFormat.Unspecified;

      bool hasMessageExpiry = false;
      uint messageExpiryVal = 0;

      bool hasTopicAlias = false;
      ushort topicAliasVal = 0;

      bool hasResponseTopic = false;
      string responseTopicVal = "";

      bool hasCorrelation = false;
      byte[] correlationVal = [];

      bool hasContentType = false;
      string contentTypeVal = "";

      bool hasUserProp = false;
      string userPropKey = "";
      string userPropVal = "";

      var propertiesSequence = options.BuildProperties();

      {
         var enumerator = new MqttPropertyEnumerator(propertiesSequence);
         while (enumerator.MoveNext())
         {
            var prop = enumerator.Current;
            switch (prop.Identifier)
            {
               case PropertyIdentifier.PayloadFormatIndicator:
                  hasPayloadFormat = true;
                  payloadFormatVal = prop.AsPayloadFormat();
                  break;
               case PropertyIdentifier.MessageExpiryInterval:
                  hasMessageExpiry = true;
                  messageExpiryVal = prop.AsMessageExpiryInterval();
                  break;
               case PropertyIdentifier.TopicAlias:
                  hasTopicAlias = true;
                  topicAliasVal = prop.AsTopicAlias();
                  break;
               case PropertyIdentifier.ResponseTopic:
                  hasResponseTopic = true;
                  var respBytes = prop.AsResponseTopic().ToArray();
                  responseTopicVal = System.Text.Encoding.UTF8.GetString(respBytes);
                  break;
               case PropertyIdentifier.CorrelationData:
                  hasCorrelation = true;
                  correlationVal = prop.AsCorrelationData().ToArray();
                  break;
               case PropertyIdentifier.ContentType:
                  hasContentType = true;
                  var ctBytes = prop.AsContentType().ToArray();
                  contentTypeVal = System.Text.Encoding.UTF8.GetString(ctBytes);
                  break;
               case PropertyIdentifier.UserProperty:
                  hasUserProp = true;
                  var userProp = prop.AsUserProperty();
                  var keyBytes = userProp.KeyBytes.ToArray();
                  var valBytes = userProp.ValueBytes.ToArray();
                  userPropKey = System.Text.Encoding.UTF8.GetString(keyBytes);
                  userPropVal = System.Text.Encoding.UTF8.GetString(valBytes);
                  break;
            }
         }
      }

      // Perform Awaited Assertions
      await Assert.That(dup).IsTrue();
      await Assert.That(qos).IsEqualTo(QualityOfServiceType.ExactlyOnce);
      await Assert.That(retain).IsTrue();
      await Assert.That(topicStr).IsEqualTo("sensor/temp");
      await Assert.That(payloadStr).IsEqualTo("hello-world");

      await Assert.That(hasPayloadFormat).IsTrue();
      await Assert.That(payloadFormatVal).IsEqualTo(PayloadFormat.CharacterData);

      await Assert.That(hasMessageExpiry).IsTrue();
      await Assert.That(messageExpiryVal).IsEqualTo(3600U);

      await Assert.That(hasTopicAlias).IsTrue();
      await Assert.That(topicAliasVal).IsEqualTo((ushort)5);

      await Assert.That(hasResponseTopic).IsTrue();
      await Assert.That(responseTopicVal).IsEqualTo("sensor/temp/response");

      await Assert.That(hasCorrelation).IsTrue();
      await Assert.That(correlationVal).IsEquivalentTo(new byte[] { 1, 2, 3 });

      await Assert.That(hasContentType).IsTrue();
      await Assert.That(contentTypeVal).IsEqualTo("text/plain");

      await Assert.That(hasUserProp).IsTrue();
      await Assert.That(userPropKey).IsEqualTo("region");
      await Assert.That(userPropVal).IsEqualTo("EU");
   }

   [Test]
   public async Task ClearResetsAllPublishOptions()
   {
      // Arrange
      var builder = new PublishOptionsBuilder()
         .WithDup(true)
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithRetain(true)
         .WithTopic("topic")
         .WithPayload("payload")
         .WithMessageExpiryInterval(60);

      var options = builder.Build();

      // Act
      options.Clear();

      // Assert
      await Assert.That(options.Dup).IsFalse();
      await Assert.That(options.QualityOfService).IsEqualTo(QualityOfServiceType.AtMostOnce);
      await Assert.That(options.Retain).IsFalse();
      await Assert.That(options.TopicUtf8Bytes.IsEmpty).IsTrue();
      await Assert.That(options.Payload.IsEmpty).IsTrue();
      await Assert.That(options.MessageExpiryInterval.HasValue).IsFalse();
      await Assert.That(options.BuildProperties().IsEmpty).IsTrue();
   }
}
