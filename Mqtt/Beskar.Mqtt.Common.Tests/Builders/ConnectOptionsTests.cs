using System;
using System.Buffers;
using System.Net;
using System.Text;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Common.Tests.Helpers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;

namespace Beskar.Mqtt.Common.Tests.Builders;

public class ConnectOptionsTests
{
   [Test]
   public async Task CorrectOptionsBuildingAndReset()
   {
      // Arrange
      var builder = new ConnectOptionsBuilder(new IPEndPoint(0, 0))
         .WithProtocolVersion(MqttProtocolVersion.V311)
         .WithCleanSession(false)
         .WithKeepAlivePeriod(30)
         .WithTimeout(TimeSpan.FromSeconds(5))
         .WithClientId("my-client-id")
         .WithUsername("my-username")
         .WithPassword("my-password")
         .WithSessionExpiryInterval(3600)
         .WithTopicAliasMaximum(10)
         .WithMaximumPacketSize(65536)
         .WithRequestResponseInformation(true)
         .WithRequestProblemInformation(false)
         .WithAuthenticationMethod("oauth")
         .WithAuthenticationData([1, 2, 3, 4])
         .WithTryPrivate(true)
         .WithWill("will-topic", [5, 6, 7], QualityOfServiceType.ExactlyOnce, retain: true)
         .WithWillDelayInterval(120)
         .WithWillPayloadFormat(PayloadFormat.CharacterData)
         .WithWillMessageExpiryInterval(600)
         .WithWillContentType("text/plain")
         .WithWillResponseTopic("will-response")
         .WithWillCorrelationData([8, 9])
         .WithWillUserProperty("will-prop-key", "will-prop-val")
         .WithUserProperty("client-prop-key", "client-prop-val");

      // Act
      var options = builder.Build();

      // Extract values to verify
      var protocolVersion = options.ProtocolVersion;
      var cleanSession = options.CleanSession;
      var keepAlive = options.KeepAlivePeriod;
      var timeout = options.Timeout;
      var clientId = Encoding.UTF8.GetString(options.ClientIdUtf8Bytes.Span);
      var username = Encoding.UTF8.GetString(options.UsernameUtf8Bytes.Span);
      var password = Encoding.UTF8.GetString(options.PasswordBytes.Span);
      var sessionExpiry = options.SessionExpiryInterval;
      var topicAliasMax = options.TopicAliasMaximum;
      var maxPacketSize = options.MaximumPacketSize;
      var reqRespInfo = options.RequestResponseInformation;
      var reqProbInfo = options.RequestProblemInformation;
      var authMethod = Encoding.UTF8.GetString(options.AuthenticationMethodUtf8Bytes.Span);
      var authData = options.AuthenticationDataBytes.ToArray();
      var tryPrivate = options.TryPrivate;

      var hasWill = options.HasWill;
      var willTopic = Encoding.UTF8.GetString(options.WillTopicUtf8Bytes.Span);
      var willPayload = options.WillPayload.ToArray();
      var willQos = options.WillQualityOfService;
      var willRetain = options.WillRetain;
      var willDelay = options.WillDelayInterval;
      var willPayloadFormat = options.WillPayloadFormatIndicator;
      var willExpiry = options.WillMessageExpiryInterval;
      var willContentType = Encoding.UTF8.GetString(options.WillContentTypeUtf8Bytes.Span);
      var willResponseTopic = Encoding.UTF8.GetString(options.WillResponseTopicUtf8Bytes.Span);
      var willCorrelation = options.WillCorrelationDataBytes.ToArray();

      var hasClientProp = false;
      var clientPropKey = "";
      var clientPropVal = "";
      var hasMoreClientProps = true;

      {
         var clientUserProps = options.UserProperties.GetEnumerator();
         if (clientUserProps.MoveNext())
         {
            hasClientProp = true;
            clientPropKey = Encoding.UTF8.GetString(clientUserProps.Current.KeyUtf8Bytes);
            clientPropVal = Encoding.UTF8.GetString(clientUserProps.Current.ValueBytes);
            hasMoreClientProps = clientUserProps.MoveNext();
         }
         else
         {
            hasMoreClientProps = false;
         }
      }

      var hasWillProp = false;
      var willPropKey = "";
      var willPropVal = "";
      var hasMoreWillProps = true;

      {
         var willUserProps = options.WillUserProperties.GetEnumerator();
         if (willUserProps.MoveNext())
         {
            hasWillProp = true;
            willPropKey = Encoding.UTF8.GetString(willUserProps.Current.KeyUtf8Bytes);
            willPropVal = Encoding.UTF8.GetString(willUserProps.Current.ValueBytes);
            hasMoreWillProps = willUserProps.MoveNext();
         }
         else
         {
            hasMoreWillProps = false;
         }
      }

      // Assert normal properties
      await Assert.That(protocolVersion).IsEqualTo(MqttProtocolVersion.V311);
      await Assert.That(cleanSession).IsFalse();
      await Assert.That(keepAlive).IsEqualTo((ushort)30);
      await Assert.That(timeout).IsEqualTo(TimeSpan.FromSeconds(5));
      await Assert.That(clientId).IsEqualTo("my-client-id");
      await Assert.That(username).IsEqualTo("my-username");
      await Assert.That(password).IsEqualTo("my-password");
      await Assert.That(sessionExpiry).IsEqualTo((uint?)3600);
      await Assert.That(topicAliasMax).IsEqualTo((ushort?)10);
      await Assert.That(maxPacketSize).IsEqualTo((uint?)65536);
      await Assert.That(reqRespInfo).IsTrue();
      await Assert.That(reqProbInfo).IsFalse();
      await Assert.That(authMethod).IsEqualTo("oauth");
      await Assert.That(authData).IsEquivalentTo(new byte[] { 1, 2, 3, 4 });
      await Assert.That(tryPrivate).IsTrue();

      // Assert Will properties
      await Assert.That(hasWill).IsTrue();
      await Assert.That(willTopic).IsEqualTo("will-topic");
      await Assert.That(willPayload).IsEquivalentTo(new byte[] { 5, 6, 7 });
      await Assert.That(willQos).IsEqualTo(QualityOfServiceType.ExactlyOnce);
      await Assert.That(willRetain).IsTrue();
      await Assert.That(willDelay).IsEqualTo((uint?)120);
      await Assert.That(willPayloadFormat).IsEqualTo(PayloadFormat.CharacterData);
      await Assert.That(willExpiry).IsEqualTo((uint?)600);
      await Assert.That(willContentType).IsEqualTo("text/plain");
      await Assert.That(willResponseTopic).IsEqualTo("will-response");
      await Assert.That(willCorrelation).IsEquivalentTo(new byte[] { 8, 9 });

      // User properties checks
      await Assert.That(hasClientProp).IsTrue();
      await Assert.That(clientPropKey).IsEqualTo("client-prop-key");
      await Assert.That(clientPropVal).IsEqualTo("client-prop-val");
      await Assert.That(hasMoreClientProps).IsFalse();

      await Assert.That(hasWillProp).IsTrue();
      await Assert.That(willPropKey).IsEqualTo("will-prop-key");
      await Assert.That(willPropVal).IsEqualTo("will-prop-val");
      await Assert.That(hasMoreWillProps).IsFalse();

      // Act: Clear
      options.Clear();

      // Assert default values after clear
      await Assert.That(options.ProtocolVersion).IsEqualTo(MqttProtocolVersion.V50);
      await Assert.That(options.CleanSession).IsTrue();
      await Assert.That(options.KeepAlivePeriod).IsEqualTo((ushort)60);
      await Assert.That(options.Timeout).IsEqualTo(TimeSpan.FromSeconds(15));
      await Assert.That(options.ClientIdUtf8Bytes.IsEmpty).IsTrue();
      await Assert.That(options.UsernameUtf8Bytes.IsEmpty).IsTrue();
      await Assert.That(options.PasswordBytes.IsEmpty).IsTrue();
      await Assert.That(options.SessionExpiryInterval).IsNull();
      await Assert.That(options.TopicAliasMaximum).IsNull();
      await Assert.That(options.MaximumPacketSize).IsNull();
      await Assert.That(options.RequestResponseInformation).IsFalse();
      await Assert.That(options.RequestProblemInformation).IsTrue();
      await Assert.That(options.AuthenticationMethodUtf8Bytes.IsEmpty).IsTrue();
      await Assert.That(options.AuthenticationDataBytes.IsEmpty).IsTrue();
      await Assert.That(options.TryPrivate).IsFalse();

      // Will defaults
      await Assert.That(options.HasWill).IsFalse();
      await Assert.That(options.WillTopicUtf8Bytes.IsEmpty).IsTrue();
      await Assert.That(options.WillPayload.IsEmpty).IsTrue();
      await Assert.That(options.WillQualityOfService).IsEqualTo(QualityOfServiceType.AtMostOnce);
      await Assert.That(options.WillRetain).IsFalse();
      await Assert.That(options.WillDelayInterval).IsNull();
      await Assert.That(options.WillPayloadFormatIndicator).IsEqualTo(PayloadFormat.Unspecified);
      await Assert.That(options.WillMessageExpiryInterval).IsNull();
      await Assert.That(options.WillContentTypeUtf8Bytes.IsEmpty).IsTrue();
      await Assert.That(options.WillResponseTopicUtf8Bytes.IsEmpty).IsTrue();
      await Assert.That(options.WillCorrelationDataBytes.IsEmpty).IsTrue();

      await Assert.That(options.UserProperties.Count).IsEqualTo(0);
      await Assert.That(options.WillUserProperties.Count).IsEqualTo(0);
   }

   [Test]
   public async Task CreateFromConnectPacketDeepCopiesEverything()
   {
      // Arrange
      var clientIdBytes = "client-id"u8.ToArray();
      var usernameBytes = "username"u8.ToArray();
      var passwordBytes = "password"u8.ToArray();
      
      var propBuffer = new MemoryBuffer();
      using (var propWriter = new ByteWriter(propBuffer.GetSpan(256)))
      {
         var propEncoder = propWriter.AsConnectPropertyEncoder();
         propEncoder.WriteSessionExpiryInterval(3600);
         propEncoder.WriteTopicAliasMaximum(10);
         propEncoder.WriteMaximumPacketSize(65536);
         propEncoder.WriteRequestResponseInformation(true);
         propEncoder.WriteRequestProblemInformation(false);
         propEncoder.WriteAuthenticationMethod("oauth"u8);
         propEncoder.WriteAuthenticationData([1, 2, 3, 4]);
         propEncoder.WriteUserProperty("client-prop-key"u8, "client-prop-val"u8);
         propBuffer.Advance(propEncoder.Encoder.Writer.Position);
      }

      var willTopicBytes = "will-topic"u8.ToArray();
      var willPayloadBytes = new byte[] { 5, 6, 7 };
      var willContentTypeBytes = "text/plain"u8.ToArray();
      var willResponseTopicBytes = "will-response"u8.ToArray();
      var willCorrelationDataBytes = new byte[] { 8, 9 };

      var willPropBuffer = new MemoryBuffer();
      using (var willPropWriter = new ByteWriter(willPropBuffer.GetSpan(256)))
      {
         var willPropEncoder = willPropWriter.AsWillPropertyEncoder();
         willPropEncoder.WritePayloadFormatIndicator(PayloadFormat.CharacterData);
         willPropEncoder.WriteMessageExpiryInterval(600);
         willPropEncoder.WriteWillDelayInterval(120);
         willPropEncoder.WriteContentType("text/plain"u8);
         willPropEncoder.WriteResponseTopic("will-response"u8);
         willPropEncoder.WriteCorrelationData([8, 9]);
         willPropEncoder.WriteUserProperty("will-prop-key"u8, "will-prop-val"u8);
         willPropBuffer.Advance(willPropEncoder.Encoder.Writer.Position);
      }

      var originalPacket = new ConnectPacket
      {
         IsCleanSession = false,
         KeepAliveInterval = 30,
         ClientIdUtf8Bytes = new ReadOnlySequence<byte>(clientIdBytes),
         UsernameUtf8Bytes = new ReadOnlySequence<byte>(usernameBytes),
         PasswordBytes = new ReadOnlySequence<byte>(passwordBytes),
         PropertiesBytes = propBuffer.WrittenSequence,
         HasWill = true,
         WillTopicUtf8Bytes = new ReadOnlySequence<byte>(willTopicBytes),
         WillMessageBytes = new ReadOnlySequence<byte>(willPayloadBytes),
         WillQualityOfService = QualityOfServiceType.ExactlyOnce,
         WillRetain = true,
         WillPropertiesBytes = willPropBuffer.WrittenSequence
      };

      // Act
      var options = ConnectOptions.Create(in originalPacket, MqttProtocolVersion.V311, new IPEndPoint(IPAddress.Loopback, 1883));

      // Verify that options has the correct properties
      await Assert.That(options.ProtocolVersion).IsEqualTo(MqttProtocolVersion.V311);
      await Assert.That(options.CleanSession).IsFalse();
      await Assert.That(options.KeepAlivePeriod).IsEqualTo((ushort)30);
      
      await Assert.That(Encoding.UTF8.GetString(options.ClientIdUtf8Bytes.Span)).IsEqualTo("client-id");
      await Assert.That(Encoding.UTF8.GetString(options.UsernameUtf8Bytes.Span)).IsEqualTo("username");
      await Assert.That(Encoding.UTF8.GetString(options.PasswordBytes.Span)).IsEqualTo("password");

      // Verify properties
      await Assert.That(options.SessionExpiryInterval).IsEqualTo((uint?)3600);
      await Assert.That(options.TopicAliasMaximum).IsEqualTo((ushort?)10);
      await Assert.That(options.MaximumPacketSize).IsEqualTo((uint?)65536);
      await Assert.That(options.RequestResponseInformation).IsTrue();
      await Assert.That(options.RequestProblemInformation).IsFalse();
      await Assert.That(Encoding.UTF8.GetString(options.AuthenticationMethodUtf8Bytes.Span)).IsEqualTo("oauth");
      await Assert.That(options.AuthenticationDataBytes.ToArray()).IsEquivalentTo(new byte[] { 1, 2, 3, 4 });

      // Verify user properties
      var hasClientProp = false;
      var clientPropKey = "";
      var clientPropVal = "";
      var hasMoreClientProps = true;

      {
         var clientUserProps = options.UserProperties.GetEnumerator();
         if (clientUserProps.MoveNext())
         {
            hasClientProp = true;
            clientPropKey = Encoding.UTF8.GetString(clientUserProps.Current.KeyUtf8Bytes);
            clientPropVal = Encoding.UTF8.GetString(clientUserProps.Current.ValueBytes);
            hasMoreClientProps = clientUserProps.MoveNext();
         }
         else
         {
            hasMoreClientProps = false;
         }
      }

      await Assert.That(hasClientProp).IsTrue();
      await Assert.That(clientPropKey).IsEqualTo("client-prop-key");
      await Assert.That(clientPropVal).IsEqualTo("client-prop-val");
      await Assert.That(hasMoreClientProps).IsFalse();

      // Verify Will
      await Assert.That(options.HasWill).IsTrue();
      await Assert.That(Encoding.UTF8.GetString(options.WillTopicUtf8Bytes.Span)).IsEqualTo("will-topic");
      await Assert.That(options.WillPayload.ToArray()).IsEquivalentTo(new byte[] { 5, 6, 7 });
      await Assert.That(options.WillQualityOfService).IsEqualTo(QualityOfServiceType.ExactlyOnce);
      await Assert.That(options.WillRetain).IsTrue();
      
      await Assert.That(options.WillPayloadFormatIndicator).IsEqualTo(PayloadFormat.CharacterData);
      await Assert.That(options.WillMessageExpiryInterval).IsEqualTo((uint?)600);
      await Assert.That(options.WillDelayInterval).IsEqualTo((uint?)120);
      await Assert.That(Encoding.UTF8.GetString(options.WillContentTypeUtf8Bytes.Span)).IsEqualTo("text/plain");
      await Assert.That(Encoding.UTF8.GetString(options.WillResponseTopicUtf8Bytes.Span)).IsEqualTo("will-response");
      await Assert.That(options.WillCorrelationDataBytes.ToArray()).IsEquivalentTo(new byte[] { 8, 9 });

      // Verify Will user properties
      var hasWillProp = false;
      var willPropKey = "";
      var willPropVal = "";
      var hasMoreWillProps = true;

      {
         var willUserProps = options.WillUserProperties.GetEnumerator();
         if (willUserProps.MoveNext())
         {
            hasWillProp = true;
            willPropKey = Encoding.UTF8.GetString(willUserProps.Current.KeyUtf8Bytes);
            willPropVal = Encoding.UTF8.GetString(willUserProps.Current.ValueBytes);
            hasMoreWillProps = willUserProps.MoveNext();
         }
         else
         {
            hasMoreWillProps = false;
         }
      }

      await Assert.That(hasWillProp).IsTrue();
      await Assert.That(willPropKey).IsEqualTo("will-prop-key");
      await Assert.That(willPropVal).IsEqualTo("will-prop-val");
      await Assert.That(hasMoreWillProps).IsFalse();

      // Corrupt original arrays to verify deep copy
      Array.Clear(clientIdBytes);
      Array.Clear(usernameBytes);
      Array.Clear(passwordBytes);
      Array.Clear(willTopicBytes);
      Array.Clear(willPayloadBytes);

      // Verify options are unaffected
      await Assert.That(Encoding.UTF8.GetString(options.ClientIdUtf8Bytes.Span)).IsEqualTo("client-id");
      await Assert.That(Encoding.UTF8.GetString(options.UsernameUtf8Bytes.Span)).IsEqualTo("username");
      await Assert.That(Encoding.UTF8.GetString(options.PasswordBytes.Span)).IsEqualTo("password");
      await Assert.That(Encoding.UTF8.GetString(options.WillTopicUtf8Bytes.Span)).IsEqualTo("will-topic");
      await Assert.That(options.WillPayload.ToArray()).IsEquivalentTo(new byte[] { 5, 6, 7 });
   }
}
