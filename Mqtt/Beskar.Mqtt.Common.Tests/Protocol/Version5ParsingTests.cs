using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Common.Parsers;
using Beskar.Mqtt.Common.Tests.Helpers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Tests.Protocol;

public class Version5ParsingTests
{
   [Test]
   public async Task CorrectPubRecParsing()
   {
      var buffer = new MemoryBuffer();
      const ushort originalPacketId = 20;
      var expectedReasonCode = PubRecReasonCode.NoMatchingSubscribers;
      var expectedReasonString = "No subscribers matched";

      var wasInvoked = false;
      ushort parsedPacketId = 0;
      var parsedReasonCode = PubRecReasonCode.Success;
      string? parsedReasonString = null;

      var handler = new TestPacketHandler
      {
         OnPubRec = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
            parsedReasonCode = p.ReasonCode;

            var reasonBytes = new byte[p.ReasonStringUtf8Bytes.Length];
            p.ReasonStringUtf8Bytes.CopyTo(reasonBytes);
            parsedReasonString = System.Text.Encoding.UTF8.GetString(reasonBytes);
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var propBuffer = new MemoryBuffer();
         var propWriter = new ByteWriter(propBuffer.GetSpan(256));
         try
         {
            var propEncoder = propWriter.AsPubAckPropertyEncoder();
            propEncoder.WriteReasonString(System.Text.Encoding.UTF8.GetBytes(expectedReasonString));
            propBuffer.Advance(propEncoder.Encoder.Writer.Position);
         }
         finally
         {
            propWriter.Dispose();
         }

         var originalPacket = new PubRecPacket
         {
            PacketIdentifier = originalPacketId,
            ReasonCode = expectedReasonCode,
            PropertiesBytes = propBuffer.WrittenSequence.ToArray()
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubRec(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
      await Assert.That(parsedReasonCode).IsEqualTo(expectedReasonCode);
      await Assert.That(parsedReasonString).IsEqualTo(expectedReasonString);
   }

   [Test]
   public async Task CorrectPubAckParsingOptimized()
   {
      var buffer = new MemoryBuffer();
      const ushort originalPacketId = 42;

      var wasInvoked = false;
      ushort parsedPacketId = 0;
      var parsedReasonCode = PubAckReasonCode.Success;

      var handler = new TestPacketHandler
      {
         OnPubAck = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
            parsedReasonCode = p.ReasonCode;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new PubAckPacket
         {
            PacketIdentifier = originalPacketId,
            ReasonCode = PubAckReasonCode.Success,
            PropertiesBytes = ReadOnlyMemory<byte>.Empty
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubAck(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      // Optimized puback with success and empty properties is exactly 4 bytes (2 fixed header + 2 packet identifier)
      await Assert.That(buffer.WrittenSpan.Length).IsEqualTo(4);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
      await Assert.That(parsedReasonCode).IsEqualTo(PubAckReasonCode.Success);
   }

   [Test]
   public async Task CorrectPublishParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      var expectedDup = true;
      var expectedQos = QualityOfServiceType.AtLeastOnce;
      var expectedRetain = true;
      var expectedPacketId = (ushort)50;
      var expectedTopic = "foo/bar";
      var expectedPayload = new byte[] { 9, 8, 7 };
      var expectedContentType = "text/plain";
      var expectedResponseTopic = "response/topic";
      var expectedSubId = 99U;

      var parsedDup = false;
      var parsedQos = QualityOfServiceType.AtMostOnce;
      var parsedRetain = false;
      ushort parsedPacketId = 0;
      string? parsedTopic = null;
      byte[]? parsedPayload = null;
      string? parsedContentType = null;
      string? parsedResponseTopic = null;
      uint parsedSubId = 0;

      var handler = new TestPacketHandler
      {
         OnPublish = (in p) =>
         {
            wasInvoked = true;
            parsedDup = p.Dup;
            parsedQos = p.QualityOfService;
            parsedRetain = p.Retain;
            parsedPacketId = p.PacketIdentifier;

            var topicBytes = new byte[p.TopicUtf8Bytes.Length];
            p.TopicUtf8Bytes.CopyTo(topicBytes);
            parsedTopic = System.Text.Encoding.UTF8.GetString(topicBytes);

            parsedPayload = new byte[p.Payload.Length];
            p.Payload.CopyTo(parsedPayload);

            var contentTypeBytes = new byte[p.ContentTypeUtf8Bytes.Length];
            p.ContentTypeUtf8Bytes.CopyTo(contentTypeBytes);
            parsedContentType = System.Text.Encoding.UTF8.GetString(contentTypeBytes);

            var respTopicBytes = new byte[p.ResponseTopicUtf8Bytes.Length];
            p.ResponseTopicUtf8Bytes.CopyTo(respTopicBytes);
            parsedResponseTopic = System.Text.Encoding.UTF8.GetString(respTopicBytes);

            parsedSubId = p.SubscriptionIdentifier;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var propBuffer = new MemoryBuffer();
         var propWriter = new ByteWriter(propBuffer.GetSpan(256));
         try
         {
            var propEncoder = propWriter.AsPublishPropertyEncoder();
            propEncoder.WriteContentType(System.Text.Encoding.UTF8.GetBytes(expectedContentType));
            propEncoder.WriteResponseTopic(System.Text.Encoding.UTF8.GetBytes(expectedResponseTopic));
            propEncoder.WriteSubscriptionIdentifier(expectedSubId);
            propBuffer.Advance(propEncoder.Encoder.Writer.Position);
         }
         finally
         {
            propWriter.Dispose();
         }

         var originalPacket = new PublishPacket
         {
            Dup = expectedDup,
            QualityOfService = expectedQos,
            Retain = expectedRetain,
            PacketIdentifier = expectedPacketId,
            TopicUtf8Bytes = new ReadOnlySequence<byte>(System.Text.Encoding.UTF8.GetBytes(expectedTopic)),
            Payload = new ReadOnlySequence<byte>(expectedPayload),
            PropertiesBytes = propBuffer.WrittenSequence
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePublish(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedDup).IsEqualTo(expectedDup);
      await Assert.That(parsedQos).IsEqualTo(expectedQos);
      await Assert.That(parsedRetain).IsEqualTo(expectedRetain);
      await Assert.That(parsedPacketId).IsEqualTo(expectedPacketId);
      await Assert.That(parsedTopic).IsEqualTo(expectedTopic);
      await Assert.That(parsedPayload).IsEquivalentTo(expectedPayload);
      await Assert.That(parsedContentType).IsEqualTo(expectedContentType);
      await Assert.That(parsedResponseTopic).IsEqualTo(expectedResponseTopic);
      await Assert.That(parsedSubId).IsEqualTo(expectedSubId);
   }

   [Test]
   public async Task CorrectConnectParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      var expectedCleanSession = true;
      var expectedKeepAlive = (ushort)60;
      var expectedClientId = "client-123";
      var expectedSessionExpiry = 3600U;
      var expectedMaxPacketSize = 65536U;

      var parsedCleanSession = false;
      ushort parsedKeepAlive = 0;
      string? parsedClientId = null;
      uint parsedSessionExpiry = 0;
      uint parsedMaxPacketSize = 0;

      var handler = new TestPacketHandler
      {
         OnConnect = (in p) =>
         {
            wasInvoked = true;
            parsedCleanSession = p.IsCleanSession;
            parsedKeepAlive = p.KeepAliveInterval;

            var clientBytes = new byte[p.ClientIdUtf8Bytes.Length];
            p.ClientIdUtf8Bytes.CopyTo(clientBytes);
            parsedClientId = System.Text.Encoding.UTF8.GetString(clientBytes);

            parsedSessionExpiry = p.SessionExpiryInterval;
            parsedMaxPacketSize = p.MaximumPacketSize;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var propBuffer = new MemoryBuffer();
         var propWriter = new ByteWriter(propBuffer.GetSpan(256));
         try
         {
            var propEncoder = propWriter.AsConnectPropertyEncoder();
            propEncoder.WriteSessionExpiryInterval(expectedSessionExpiry);
            propEncoder.WriteMaximumPacketSize(expectedMaxPacketSize);
            propBuffer.Advance(propEncoder.Encoder.Writer.Position);
         }
         finally
         {
            propWriter.Dispose();
         }

         var originalPacket = new ConnectPacket
         {
            IsCleanSession = expectedCleanSession,
            KeepAliveInterval = expectedKeepAlive,
            ClientIdUtf8Bytes = new ReadOnlySequence<byte>(System.Text.Encoding.UTF8.GetBytes(expectedClientId)),
            PropertiesBytes = propBuffer.WrittenSequence
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteConnect(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.Unknown);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedCleanSession).IsEqualTo(expectedCleanSession);
      await Assert.That(parsedKeepAlive).IsEqualTo(expectedKeepAlive);
      await Assert.That(parsedClientId).IsEqualTo(expectedClientId);
      await Assert.That(parsedSessionExpiry).IsEqualTo(expectedSessionExpiry);
      await Assert.That(parsedMaxPacketSize).IsEqualTo(expectedMaxPacketSize);
   }

   [Test]
   public async Task CorrectConnAckParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      var expectedSessionPresent = true;
      var expectedReasonCode = ConnectReasonCode.ServerMoved;
      var expectedMaxQos = QualityOfServiceType.AtLeastOnce;
      var expectedAssignedClientId = "client-assigned-456";

      var parsedSessionPresent = false;
      var parsedReasonCode = ConnectReasonCode.Success;
      var parsedMaxQos = QualityOfServiceType.ExactlyOnce;
      string? parsedAssignedClientId = null;

      var handler = new TestPacketHandler
      {
         OnConnAck = (in p) =>
         {
            wasInvoked = true;
            parsedSessionPresent = p.IsSessionPresent;
            parsedReasonCode = p.ReasonCode;
            parsedMaxQos = p.MaximumQualityOfService;

            var assignedBytes = new byte[p.AssignedClientIdentifierUtf8Bytes.Length];
            p.AssignedClientIdentifierUtf8Bytes.CopyTo(assignedBytes);
            parsedAssignedClientId = System.Text.Encoding.UTF8.GetString(assignedBytes);
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var propBuffer = new MemoryBuffer();
         var propWriter = new ByteWriter(propBuffer.GetSpan(256));
         try
         {
            var propEncoder = propWriter.AsConnAckPropertyEncoder();
            propEncoder.WriteMaximumQoS(expectedMaxQos);
            propEncoder.WriteAssignedClientIdentifier(System.Text.Encoding.UTF8.GetBytes(expectedAssignedClientId));
            propBuffer.Advance(propEncoder.Encoder.Writer.Position);
         }
         finally
         {
            propWriter.Dispose();
         }

         var originalPacket = new ConnAckPacket
         {
            IsSessionPresent = expectedSessionPresent,
            ReasonCode = expectedReasonCode,
            PropertiesBytes = propBuffer.WrittenSequence
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteConnAck(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedSessionPresent).IsEqualTo(expectedSessionPresent);
      await Assert.That(parsedReasonCode).IsEqualTo(expectedReasonCode);
      await Assert.That(parsedMaxQos).IsEqualTo(expectedMaxQos);
      await Assert.That(parsedAssignedClientId).IsEqualTo(expectedAssignedClientId);
   }

   [Test]
   public async Task CorrectDisconnectParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      var expectedReasonCode = DisconnectReasonCode.SessionTakenOver;
      var expectedServerRef = "backup.server.com";

      var parsedReasonCode = DisconnectReasonCode.NormalDisconnection;
      string? parsedServerRef = null;

      var handler = new TestPacketHandler
      {
         OnDisconnect = (in p) =>
         {
            wasInvoked = true;
            parsedReasonCode = p.ReasonCode;

            var serverRefBytes = new byte[p.ServerReferenceUtf8Bytes.Length];
            p.ServerReferenceUtf8Bytes.CopyTo(serverRefBytes);
            parsedServerRef = System.Text.Encoding.UTF8.GetString(serverRefBytes);
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var propBuffer = new MemoryBuffer();
         var propWriter = new ByteWriter(propBuffer.GetSpan(256));
         try
         {
            var propEncoder = propWriter.AsDisconnectPropertyEncoder();
            propEncoder.WriteServerReference(System.Text.Encoding.UTF8.GetBytes(expectedServerRef));
            propBuffer.Advance(propEncoder.Encoder.Writer.Position);
         }
         finally
         {
            propWriter.Dispose();
         }

         var originalPacket = new DisconnectPacket
         {
            ReasonCode = expectedReasonCode,
            PropertiesBytes = propBuffer.WrittenSequence
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteDisconnect(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedReasonCode).IsEqualTo(expectedReasonCode);
      await Assert.That(parsedServerRef).IsEqualTo(expectedServerRef);
   }

   [Test]
   public async Task CorrectAuthParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      var expectedReasonCode = AuthenticateReasonCode.ContinueAuthentication;
      var expectedMethod = "SCRAM-SHA-256";
      var expectedData = new byte[] { 10, 20, 30 };

      var parsedReasonCode = AuthenticateReasonCode.Success;
      string? parsedMethod = null;
      byte[]? parsedData = null;

      var handler = new TestPacketHandler
      {
         OnAuth = (in p) =>
         {
            wasInvoked = true;
            parsedReasonCode = p.ReasonCode;

            var methodBytes = new byte[p.AuthenticationMethodUtf8Bytes.Length];
            p.AuthenticationMethodUtf8Bytes.CopyTo(methodBytes);
            parsedMethod = System.Text.Encoding.UTF8.GetString(methodBytes);

            parsedData = new byte[p.AuthenticationDataBytes.Length];
            p.AuthenticationDataBytes.CopyTo(parsedData);
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var propBuffer = new MemoryBuffer();
         var propWriter = new ByteWriter(propBuffer.GetSpan(256));
         try
         {
            var propEncoder = propWriter.AsAuthPropertyEncoder();
            propEncoder.WriteAuthenticationMethod(System.Text.Encoding.UTF8.GetBytes(expectedMethod));
            propEncoder.WriteAuthenticationData(expectedData);
            propBuffer.Advance(propEncoder.Encoder.Writer.Position);
         }
         finally
         {
            propWriter.Dispose();
         }

         var originalPacket = new AuthPacket
         {
            ReasonCode = expectedReasonCode,
            PropertiesBytes = propBuffer.WrittenSequence
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteAuth(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedReasonCode).IsEqualTo(expectedReasonCode);
      await Assert.That(parsedMethod).IsEqualTo(expectedMethod);
      await Assert.That(parsedData).IsEquivalentTo(expectedData);
   }

   [Test]
   public async Task CorrectSubscribeParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      ushort expectedPacketId = 88;
      var expectedSubId = 7U;
      // SUBSCRIBE payload: topic "test" (length 4) + option byte (Max QoS = 1, NL = 0, RAP = 0, Retain Handling = 0 -> 0x01)
      var expectedFiltersBytes = new byte[] { 0x00, 0x04, (byte)'t', (byte)'e', (byte)'s', (byte)'t', 0x01 };

      ushort parsedPacketId = 0;
      byte[]? parsedFilters = null;
      uint parsedSubId = 0;

      var handler = new TestPacketHandler
      {
         OnSubscribe = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
            parsedSubId = p.SubscriptionIdentifier;

            parsedFilters = new byte[p.FiltersBytes.Length];
            p.FiltersBytes.CopyTo(parsedFilters);
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var propBuffer = new MemoryBuffer();
         var propWriter = new ByteWriter(propBuffer.GetSpan(256));
         try
         {
            var propEncoder = propWriter.AsSubscribePropertyEncoder();
            propEncoder.WriteSubscriptionIdentifier(expectedSubId);
            propBuffer.Advance(propEncoder.Encoder.Writer.Position);
         }
         finally
         {
            propWriter.Dispose();
         }

         var originalPacket = new SubscribePacket
         {
            PacketIdentifier = expectedPacketId,
            FiltersBytes = new ReadOnlySequence<byte>(expectedFiltersBytes),
            PropertiesBytes = propBuffer.WrittenSequence
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteSubscribe(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(expectedPacketId);
      await Assert.That(parsedSubId).IsEqualTo(expectedSubId);
      await Assert.That(parsedFilters).IsEquivalentTo(expectedFiltersBytes);
   }

   [Test]
   public async Task CorrectSubscribeParsingWithOptions()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      ushort expectedPacketId = 89;
      // SUBSCRIBE payload: topic "test" (length 4) + option byte: Max QoS = 1, NL = 1, RAP = 1, Retain Handling = 2 -> 0x2D
      var expectedFiltersBytes = new byte[] { 0x00, 0x04, (byte)'t', (byte)'e', (byte)'s', (byte)'t', 0x2D };

      QualityOfServiceType parsedQos = QualityOfServiceType.AtMostOnce;
      bool parsedNoLocal = false;
      bool parsedRetainAsPublished = false;
      RetainHandlingType parsedRetainHandling = RetainHandlingType.SendAtSubscription;

      var handler = new TestPacketHandler
      {
         OnSubscribe = (in p) =>
         {
            wasInvoked = true;
            var enumerator = p.GetFilters();
            if (enumerator.MoveNext())
            {
               var filter = enumerator.Current;
               parsedQos = filter.QualityOfService;
               parsedNoLocal = filter.NoLocal;
               parsedRetainAsPublished = filter.RetainAsPublished;
               parsedRetainHandling = filter.RetainHandling;
            }
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new SubscribePacket
         {
            PacketIdentifier = expectedPacketId,
            FiltersBytes = new ReadOnlySequence<byte>(expectedFiltersBytes),
            PropertiesBytes = ReadOnlySequence<byte>.Empty
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteSubscribe(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedQos).IsEqualTo(QualityOfServiceType.AtLeastOnce);
      await Assert.That(parsedNoLocal).IsTrue();
      await Assert.That(parsedRetainAsPublished).IsTrue();
      await Assert.That(parsedRetainHandling).IsEqualTo(RetainHandlingType.DoNotSend);
   }

   [Test]
   public async Task InvalidSubscribeParsingWithRetainHandling3()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      ushort expectedPacketId = 90;
      // SUBSCRIBE payload: topic "test" (length 4) + option byte: Max QoS = 1, Retain Handling = 3 (invalid) -> 0x31
      var expectedFiltersBytes = new byte[] { 0x00, 0x04, (byte)'t', (byte)'e', (byte)'s', (byte)'t', 0x31 };

      var handler = new TestPacketHandler
      {
         OnSubscribe = (in p) =>
         {
            wasInvoked = true;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new SubscribePacket
         {
            PacketIdentifier = expectedPacketId,
            FiltersBytes = new ReadOnlySequence<byte>(expectedFiltersBytes),
            PropertiesBytes = ReadOnlySequence<byte>.Empty
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteSubscribe(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.ProtocolError);
      await Assert.That(wasInvoked).IsFalse();
   }

   [Test]
   public async Task CorrectUnsubscribeParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      ushort expectedPacketId = 111;
      var expectedFiltersBytes = new byte[] { 0x00, 0x04, (byte)'t', (byte)'e', (byte)'s', (byte)'t' };

      ushort parsedPacketId = 0;
      byte[]? parsedFilters = null;

      var handler = new TestPacketHandler
      {
         OnUnsubscribe = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;

            parsedFilters = new byte[p.FiltersBytes.Length];
            p.FiltersBytes.CopyTo(parsedFilters);
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new UnsubscribePacket
         {
            PacketIdentifier = expectedPacketId,
            FiltersBytes = new ReadOnlySequence<byte>(expectedFiltersBytes),
            PropertiesBytes = ReadOnlySequence<byte>.Empty
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteUnsubscribe(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(expectedPacketId);
      await Assert.That(parsedFilters).IsEquivalentTo(expectedFiltersBytes);
   }

   [Test]
   public async Task CorrectPubAckParsing()
   {
      var buffer = new MemoryBuffer();
      const ushort originalPacketId = 42;
      var expectedReasonCode = PubAckReasonCode.NoMatchingSubscribers;
      var expectedReasonString = "No subscribers matched";

      var wasInvoked = false;
      ushort parsedPacketId = 0;
      var parsedReasonCode = PubAckReasonCode.Success;
      string? parsedReasonString = null;

      var handler = new TestPacketHandler
      {
         OnPubAck = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
            parsedReasonCode = p.ReasonCode;

            var reasonBytes = new byte[p.ReasonStringUtf8Bytes.Length];
            p.ReasonStringUtf8Bytes.CopyTo(reasonBytes);
            parsedReasonString = System.Text.Encoding.UTF8.GetString(reasonBytes);
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var propBuffer = new MemoryBuffer();
         var propWriter = new ByteWriter(propBuffer.GetSpan(256));
         try
         {
            var propEncoder = propWriter.AsPubAckPropertyEncoder();
            propEncoder.WriteReasonString(System.Text.Encoding.UTF8.GetBytes(expectedReasonString));
            propBuffer.Advance(propEncoder.Encoder.Writer.Position);
         }
         finally
         {
            propWriter.Dispose();
         }

          var originalPacket = new PubAckPacket
          {
             PacketIdentifier = originalPacketId,
             ReasonCode = expectedReasonCode,
             PropertiesBytes = propBuffer.WrittenSequence.ToArray()
          };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubAck(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
      await Assert.That(parsedReasonCode).IsEqualTo(expectedReasonCode);
      await Assert.That(parsedReasonString).IsEqualTo(expectedReasonString);
   }

   [Test]
   public async Task CorrectPubRecParsingOptimized()
   {
      var buffer = new MemoryBuffer();
      const ushort originalPacketId = 20;

      var wasInvoked = false;
      ushort parsedPacketId = 0;
      var parsedReasonCode = PubRecReasonCode.Success;

      var handler = new TestPacketHandler
      {
         OnPubRec = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
            parsedReasonCode = p.ReasonCode;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
          var originalPacket = new PubRecPacket
          {
             PacketIdentifier = originalPacketId,
             ReasonCode = PubRecReasonCode.Success,
             PropertiesBytes = ReadOnlyMemory<byte>.Empty
          };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubRec(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(buffer.WrittenSpan.Length).IsEqualTo(4);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
      await Assert.That(parsedReasonCode).IsEqualTo(PubRecReasonCode.Success);
   }

   [Test]
   public async Task CorrectPubRelParsing()
   {
      var buffer = new MemoryBuffer();
      const ushort originalPacketId = 44;
      var expectedReasonCode = PubRelReasonCode.PacketIdentifierNotFound;
      var expectedReasonString = "Packet not found";

      var wasInvoked = false;
      ushort parsedPacketId = 0;
      var parsedReasonCode = PubRelReasonCode.Success;
      string? parsedReasonString = null;

      var handler = new TestPacketHandler
      {
         OnPubRel = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
            parsedReasonCode = p.ReasonCode;

            var reasonBytes = new byte[p.ReasonStringUtf8Bytes.Length];
            p.ReasonStringUtf8Bytes.CopyTo(reasonBytes);
            parsedReasonString = System.Text.Encoding.UTF8.GetString(reasonBytes);
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var propBuffer = new MemoryBuffer();
         var propWriter = new ByteWriter(propBuffer.GetSpan(256));
         try
         {
            var propEncoder = propWriter.AsPubAckPropertyEncoder();
            propEncoder.WriteReasonString(System.Text.Encoding.UTF8.GetBytes(expectedReasonString));
            propBuffer.Advance(propEncoder.Encoder.Writer.Position);
         }
         finally
         {
            propWriter.Dispose();
         }

          var originalPacket = new PubRelPacket
          {
             PacketIdentifier = originalPacketId,
             ReasonCode = expectedReasonCode,
             PropertiesBytes = propBuffer.WrittenSequence.ToArray()
          };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubRel(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
      await Assert.That(parsedReasonCode).IsEqualTo(expectedReasonCode);
      await Assert.That(parsedReasonString).IsEqualTo(expectedReasonString);
   }

   [Test]
   public async Task CorrectPubRelParsingOptimized()
   {
      var buffer = new MemoryBuffer();
      const ushort originalPacketId = 44;

      var wasInvoked = false;
      ushort parsedPacketId = 0;
      var parsedReasonCode = PubRelReasonCode.Success;

      var handler = new TestPacketHandler
      {
         OnPubRel = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
            parsedReasonCode = p.ReasonCode;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
          var originalPacket = new PubRelPacket
          {
             PacketIdentifier = originalPacketId,
             ReasonCode = PubRelReasonCode.Success,
             PropertiesBytes = ReadOnlyMemory<byte>.Empty
          };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubRel(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(buffer.WrittenSpan.Length).IsEqualTo(4);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
      await Assert.That(parsedReasonCode).IsEqualTo(PubRelReasonCode.Success);
   }

   [Test]
   public async Task CorrectPubCompParsing()
   {
      var buffer = new MemoryBuffer();
      const ushort originalPacketId = 43;
      var expectedReasonCode = PubCompReasonCode.PacketIdentifierNotFound;
      var expectedReasonString = "Packet not found";

      var wasInvoked = false;
      ushort parsedPacketId = 0;
      var parsedReasonCode = PubCompReasonCode.Success;
      string? parsedReasonString = null;

      var handler = new TestPacketHandler
      {
         OnPubComp = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
            parsedReasonCode = p.ReasonCode;

            var reasonBytes = new byte[p.ReasonStringUtf8Bytes.Length];
            p.ReasonStringUtf8Bytes.CopyTo(reasonBytes);
            parsedReasonString = System.Text.Encoding.UTF8.GetString(reasonBytes);
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var propBuffer = new MemoryBuffer();
         var propWriter = new ByteWriter(propBuffer.GetSpan(256));
         try
         {
            var propEncoder = propWriter.AsPubAckPropertyEncoder();
            propEncoder.WriteReasonString(System.Text.Encoding.UTF8.GetBytes(expectedReasonString));
            propBuffer.Advance(propEncoder.Encoder.Writer.Position);
         }
         finally
         {
            propWriter.Dispose();
         }

         var originalPacket = new PubCompPacket
         {
            PacketIdentifier = originalPacketId,
            ReasonCode = expectedReasonCode,
            PropertiesBytes = propBuffer.WrittenSequence.ToArray()
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubComp(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
      await Assert.That(parsedReasonCode).IsEqualTo(expectedReasonCode);
      await Assert.That(parsedReasonString).IsEqualTo(expectedReasonString);
   }

   [Test]
   public async Task CorrectPubCompParsingOptimized()
   {
      var buffer = new MemoryBuffer();
      const ushort originalPacketId = 43;

      var wasInvoked = false;
      ushort parsedPacketId = 0;
      var parsedReasonCode = PubCompReasonCode.Success;

      var handler = new TestPacketHandler
      {
         OnPubComp = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
            parsedReasonCode = p.ReasonCode;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new PubCompPacket
         {
            PacketIdentifier = originalPacketId,
            ReasonCode = PubCompReasonCode.Success,
            PropertiesBytes = ReadOnlyMemory<byte>.Empty
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubComp(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(buffer.WrittenSpan.Length).IsEqualTo(4);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
      await Assert.That(parsedReasonCode).IsEqualTo(PubCompReasonCode.Success);
   }

   [Test]
   public async Task CorrectSubAckParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      ushort expectedPacketId = 77;
      var expectedReturnCodes = new byte[] { 0x00, 0x01, 0x80 };
      var expectedReasonString = "Subscribed successfully";

      ushort parsedPacketId = 0;
      byte[]? parsedReturnCodes = null;
      string? parsedReasonString = null;

      var handler = new TestPacketHandler
      {
         OnSubAck = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;

            parsedReturnCodes = new byte[p.ReturnCodesBytes.Length];
            p.ReturnCodesBytes.CopyTo(parsedReturnCodes);

            var reasonBytes = new byte[p.ReasonStringUtf8Bytes.Length];
            p.ReasonStringUtf8Bytes.CopyTo(reasonBytes);
            parsedReasonString = System.Text.Encoding.UTF8.GetString(reasonBytes);
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var propBuffer = new MemoryBuffer();
         var propWriter = new ByteWriter(propBuffer.GetSpan(256));
         try
         {
            var propEncoder = propWriter.AsSubAckPropertyEncoder();
            propEncoder.WriteReasonString(System.Text.Encoding.UTF8.GetBytes(expectedReasonString));
            propBuffer.Advance(propEncoder.Encoder.Writer.Position);
         }
         finally
         {
            propWriter.Dispose();
         }

         var originalPacket = new SubAckPacket
         {
            PacketIdentifier = expectedPacketId,
            ReturnCodesBytes = expectedReturnCodes,
            PropertiesBytes = propBuffer.WrittenSequence.ToArray()
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteSubAck(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(expectedPacketId);
      await Assert.That(parsedReturnCodes).IsEquivalentTo(expectedReturnCodes);
      await Assert.That(parsedReasonString).IsEqualTo(expectedReasonString);
   }

   [Test]
   public async Task CorrectUnsubAckParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      ushort expectedPacketId = 99;
      var expectedReasonCodes = new byte[] { 0x00, 0x11, 0x80 };
      var expectedReasonString = "Unsubscribed successfully";

      ushort parsedPacketId = 0;
      byte[]? parsedReasonCodes = null;
      string? parsedReasonString = null;

      var handler = new TestPacketHandler
      {
         OnUnsubAck = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;

            parsedReasonCodes = new byte[p.ReasonCodesBytes.Length];
            p.ReasonCodesBytes.CopyTo(parsedReasonCodes);

            var reasonBytes = new byte[p.ReasonStringUtf8Bytes.Length];
            p.ReasonStringUtf8Bytes.CopyTo(reasonBytes);
            parsedReasonString = System.Text.Encoding.UTF8.GetString(reasonBytes);
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var propBuffer = new MemoryBuffer();
         var propWriter = new ByteWriter(propBuffer.GetSpan(256));
         try
         {
            var propEncoder = propWriter.AsUnsubAckPropertyEncoder();
            propEncoder.WriteReasonString(System.Text.Encoding.UTF8.GetBytes(expectedReasonString));
            propBuffer.Advance(propEncoder.Encoder.Writer.Position);
         }
         finally
         {
            propWriter.Dispose();
         }

         var originalPacket = new UnsubAckPacket
         {
            PacketIdentifier = expectedPacketId,
            ReasonCodesBytes = expectedReasonCodes,
            PropertiesBytes = propBuffer.WrittenSequence.ToArray()
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteUnsubAck(originalPacket);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(expectedPacketId);
      await Assert.That(parsedReasonCodes).IsEquivalentTo(expectedReasonCodes);
      await Assert.That(parsedReasonString).IsEqualTo(expectedReasonString);
   }

   [Test]
   public async Task CorrectPublishHeapEncodingAndParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      var expectedDup = true;
      var expectedQos = QualityOfServiceType.AtLeastOnce;
      var expectedRetain = true;
      var expectedPacketId = (ushort)55;
      var expectedTopic = "heap/topic";
      var expectedPayload = new byte[] { 1, 2, 3, 4 };
      var expectedContentType = "application/json";
      var expectedResponseTopic = "response/heap";
      var expectedPayloadFormat = PayloadFormat.CharacterData;
      var expectedMessageExpiry = 120U;
      var expectedTopicAlias = (ushort)5;
      var expectedSubIds = new List<uint> { 77, 88 };

      var parsedDup = false;
      var parsedQos = QualityOfServiceType.AtMostOnce;
      var parsedRetain = false;
      ushort parsedPacketId = 0;
      string? parsedTopic = null;
      byte[]? parsedPayload = null;
      string? parsedContentType = null;
      string? parsedResponseTopic = null;
      PayloadFormat parsedPayloadFormat = PayloadFormat.Unspecified;
      uint parsedMessageExpiry = 0;
      ushort parsedTopicAlias = 0;
      var hasUserProp = false;
      var parsedSubIds = new List<uint>();

      var handler = new TestPacketHandler
      {
         OnPublish = (in p) =>
         {
            wasInvoked = true;
            parsedDup = p.Dup;
            parsedQos = p.QualityOfService;
            parsedRetain = p.Retain;
            parsedPacketId = p.PacketIdentifier;

            var topicBytes = new byte[p.TopicUtf8Bytes.Length];
            p.TopicUtf8Bytes.CopyTo(topicBytes);
            parsedTopic = System.Text.Encoding.UTF8.GetString(topicBytes);

            parsedPayload = new byte[p.Payload.Length];
            p.Payload.CopyTo(parsedPayload);

            var contentTypeBytes = new byte[p.ContentTypeUtf8Bytes.Length];
            p.ContentTypeUtf8Bytes.CopyTo(contentTypeBytes);
            parsedContentType = System.Text.Encoding.UTF8.GetString(contentTypeBytes);

            var respTopicBytes = new byte[p.ResponseTopicUtf8Bytes.Length];
            p.ResponseTopicUtf8Bytes.CopyTo(respTopicBytes);
            parsedResponseTopic = System.Text.Encoding.UTF8.GetString(respTopicBytes);

            parsedPayloadFormat = p.PayloadFormat;
            parsedMessageExpiry = p.MessageExpiryInterval;
            parsedTopicAlias = p.TopicAlias;

            var subIdEnumerator = p.GetSubscriptionIdentifiers();
            while (subIdEnumerator.MoveNext())
            {
               parsedSubIds.Add(subIdEnumerator.Current);
            }

            var propertiesEnumerator = p.GetProperties();
            while (propertiesEnumerator.MoveNext())
            {
               var prop = propertiesEnumerator.Current;
               if (prop.Identifier == PropertyIdentifier.UserProperty)
               {
                  var pair = prop.AsUserProperty();
                  var keyBytes = new byte[pair.KeyBytes.Length];
                  pair.KeyBytes.CopyTo(keyBytes);
                  var key = System.Text.Encoding.UTF8.GetString(keyBytes);

                  var valBytes = new byte[pair.ValueBytes.Length];
                  pair.ValueBytes.CopyTo(valBytes);
                  var val = System.Text.Encoding.UTF8.GetString(valBytes);

                  if (key == "user-key" && val == "user-val")
                  {
                     hasUserProp = true;
                  }
               }
            }
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var options = PublishOptions.Create()
            .WithDup(expectedDup)
            .WithQualityOfService(expectedQos)
            .WithRetain(expectedRetain)
            .WithTopic(expectedTopic)
            .WithPayload(expectedPayload)
            .WithPayloadFormat(expectedPayloadFormat)
            .WithMessageExpiryInterval(expectedMessageExpiry)
            .WithTopicAlias(expectedTopicAlias)
            .WithContentType(expectedContentType)
            .WithResponseTopic(expectedResponseTopic)
            .WithUserProperty("user-key", "user-val")
            .WithSubscriptionIdentifier(77)
            .WithSubscriptionIdentifier(88)
            .Build();

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePublish(options, expectedPacketId);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedDup).IsEqualTo(expectedDup);
      await Assert.That(parsedQos).IsEqualTo(expectedQos);
      await Assert.That(parsedRetain).IsEqualTo(expectedRetain);
      await Assert.That(parsedPacketId).IsEqualTo(expectedPacketId);
      await Assert.That(parsedTopic).IsEqualTo(expectedTopic);
      await Assert.That(parsedPayload).IsEquivalentTo(expectedPayload);
      await Assert.That(parsedContentType).IsEqualTo(expectedContentType);
      await Assert.That(parsedResponseTopic).IsEqualTo(expectedResponseTopic);
      await Assert.That(parsedPayloadFormat).IsEqualTo(expectedPayloadFormat);
      await Assert.That(parsedMessageExpiry).IsEqualTo(expectedMessageExpiry);
      await Assert.That(parsedTopicAlias).IsEqualTo(expectedTopicAlias);
      await Assert.That(hasUserProp).IsTrue();
      await Assert.That(parsedSubIds).IsEquivalentTo(expectedSubIds);
   }

   [Test]
   public async Task CorrectSubscribeHeapEncodingAndParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      ushort expectedPacketId = 91;
      var expectedSubId = 12U;
      var expectedTopic = "heap/sub";
      var expectedQos = QualityOfServiceType.AtLeastOnce;
      var expectedNoLocal = true;
      var expectedRetainAsPublished = true;
      var expectedRetainHandling = RetainHandlingType.SendAtSubscription;
      var hasUserProp = false;

      ushort parsedPacketId = 0;
      uint parsedSubId = 0;
      var parsedQos = QualityOfServiceType.AtMostOnce;
      var parsedNoLocal = false;
      var parsedRetainAsPublished = false;
      var parsedRetainHandling = RetainHandlingType.SendAtSubscription;
      string? parsedTopic = null;

      var handler = new TestPacketHandler
      {
         OnSubscribe = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
            parsedSubId = p.SubscriptionIdentifier;

            var enumerator = p.GetFilters();
            if (enumerator.MoveNext())
            {
               var filter = enumerator.Current;
               var topicBytes = new byte[filter.TopicUtf8Bytes.Length];
               filter.TopicUtf8Bytes.CopyTo(topicBytes);
               parsedTopic = System.Text.Encoding.UTF8.GetString(topicBytes);

               parsedQos = filter.QualityOfService;
               parsedNoLocal = filter.NoLocal;
               parsedRetainAsPublished = filter.RetainAsPublished;
               parsedRetainHandling = filter.RetainHandling;
            }

            var propertiesEnumerator = p.GetProperties();
            while (propertiesEnumerator.MoveNext())
            {
               var prop = propertiesEnumerator.Current;
               if (prop.Identifier == PropertyIdentifier.UserProperty)
               {
                  var pair = prop.AsUserProperty();
                  var keyBytes = new byte[pair.KeyBytes.Length];
                  pair.KeyBytes.CopyTo(keyBytes);
                  var key = System.Text.Encoding.UTF8.GetString(keyBytes);

                  var valBytes = new byte[pair.ValueBytes.Length];
                  pair.ValueBytes.CopyTo(valBytes);
                  var val = System.Text.Encoding.UTF8.GetString(valBytes);

                  if (key == "sub-key" && val == "sub-val")
                  {
                     hasUserProp = true;
                  }
               }
            }
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var options = new SubscribeOptions();
         options.SubscriptionIdentifier = expectedSubId;
         options.TopicFilters.Add(expectedTopic, expectedQos, expectedNoLocal, expectedRetainAsPublished, expectedRetainHandling);
         options.UserProperties.Add("sub-key", "sub-val");

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteSubscribe(options, expectedPacketId);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(expectedPacketId);
      await Assert.That(parsedSubId).IsEqualTo(expectedSubId);
      await Assert.That(parsedTopic).IsEqualTo(expectedTopic);
      await Assert.That(parsedQos).IsEqualTo(expectedQos);
      await Assert.That(parsedNoLocal).IsEqualTo(expectedNoLocal);
      await Assert.That(parsedRetainAsPublished).IsEqualTo(expectedRetainAsPublished);
      await Assert.That(parsedRetainHandling).IsEqualTo(expectedRetainHandling);
      await Assert.That(hasUserProp).IsTrue();
   }

   [Test]
   public async Task CorrectUnsubscribeHeapEncodingAndParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      ushort expectedPacketId = 112;
      var expectedTopic1 = "unsub/1";
      var expectedTopic2 = "unsub/2";
      var hasUserProp = false;

      ushort parsedPacketId = 0;
      var parsedFilters = new System.Collections.Generic.List<string>();

      var handler = new TestPacketHandler
      {
         OnUnsubscribe = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;

            var enumerator = p.GetFilters();
            while (enumerator.MoveNext())
            {
               var filterBytes = new byte[enumerator.Current.Length];
               enumerator.Current.CopyTo(filterBytes);
               parsedFilters.Add(System.Text.Encoding.UTF8.GetString(filterBytes));
            }

            var propertiesEnumerator = p.GetProperties();
            while (propertiesEnumerator.MoveNext())
            {
               var prop = propertiesEnumerator.Current;
               if (prop.Identifier == PropertyIdentifier.UserProperty)
               {
                  var pair = prop.AsUserProperty();
                  var keyBytes = new byte[pair.KeyBytes.Length];
                  pair.KeyBytes.CopyTo(keyBytes);
                  var key = System.Text.Encoding.UTF8.GetString(keyBytes);

                  var valBytes = new byte[pair.ValueBytes.Length];
                  pair.ValueBytes.CopyTo(valBytes);
                  var val = System.Text.Encoding.UTF8.GetString(valBytes);

                  if (key == "unsub-key" && val == "unsub-val")
                  {
                     hasUserProp = true;
                  }
               }
            }
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var options = new UnsubscribeOptions();
         options.TopicFilters.Add(expectedTopic1);
         options.TopicFilters.Add(expectedTopic2);
         options.UserProperties.Add("unsub-key", "unsub-val");

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteUnsubscribe(options, expectedPacketId);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(expectedPacketId);
      await Assert.That(parsedFilters.Count).IsEqualTo(2);
      await Assert.That(parsedFilters[0]).IsEqualTo(expectedTopic1);
      await Assert.That(parsedFilters[1]).IsEqualTo(expectedTopic2);
      await Assert.That(hasUserProp).IsTrue();
   }

   [Test]
   public async Task CorrectConnectHeapEncodingAndParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      var expectedCleanSession = true;
      var expectedKeepAlive = (ushort)60;
      var expectedClientId = "client-heap-v5";
      var expectedHasWill = true;
      var expectedWillQos = QualityOfServiceType.ExactlyOnce;
      var expectedWillRetain = true;
      var expectedWillTopic = "will/topic/v5";
      var expectedWillMessage = new byte[] { 1, 3, 5, 7 };
      var expectedUsername = "admin-v5";
      var expectedPassword = new byte[] { 2, 4, 6, 8 };

      var expectedSessionExpiry = 3600U;
      var expectedTopicAliasMax = (ushort)10;
      var expectedMaxPacketSize = 1024U;
      var expectedRequestResponseInfo = true;
      var expectedRequestProblemInfo = false;
      var expectedAuthMethod = "auth-v5";
      var expectedAuthData = new byte[] { 9, 9, 9 };

      var expectedWillDelay = 10U;
      var expectedWillPayloadFormat = PayloadFormat.CharacterData;
      var expectedWillExpiry = 180U;
      var expectedWillContentType = "text/plain";
      var expectedWillRespTopic = "will/resp";
      var expectedWillCorrData = new byte[] { 5, 5, 5 };

      var parsedCleanSession = false;
      ushort parsedKeepAlive = 0;
      string? parsedClientId = null;
      var parsedHasWill = false;
      var parsedWillQos = QualityOfServiceType.AtMostOnce;
      var parsedWillRetain = false;
      string? parsedWillTopic = null;
      byte[]? parsedWillMessage = null;
      string? parsedUsername = null;
      byte[]? parsedPassword = null;

      uint parsedSessionExpiry = 0;
      ushort parsedTopicAliasMax = 0;
      uint parsedMaxPacketSize = 0;
      var parsedRequestResponseInfo = false;
      var parsedRequestProblemInfo = true;
      string? parsedAuthMethod = null;
      byte[]? parsedAuthData = null;

      uint parsedWillDelay = 0;
      var parsedWillPayloadFormat = PayloadFormat.Unspecified;
      uint parsedWillExpiry = 0;
      string? parsedWillContentType = null;
      string? parsedWillRespTopic = null;
      byte[]? parsedWillCorrData = null;

      var hasUserProp = false;
      var hasWillUserProp = false;

      var handler = new TestPacketHandler
      {
         OnConnect = (in p) =>
         {
            wasInvoked = true;
            parsedCleanSession = p.IsCleanSession;
            parsedKeepAlive = p.KeepAliveInterval;

            var clientBytes = new byte[p.ClientIdUtf8Bytes.Length];
            p.ClientIdUtf8Bytes.CopyTo(clientBytes);
            parsedClientId = System.Text.Encoding.UTF8.GetString(clientBytes);

            parsedHasWill = p.HasWill;
            parsedWillQos = p.WillQualityOfService;
            parsedWillRetain = p.WillRetain;

            var willTopicBytes = new byte[p.WillTopicUtf8Bytes.Length];
            p.WillTopicUtf8Bytes.CopyTo(willTopicBytes);
            parsedWillTopic = System.Text.Encoding.UTF8.GetString(willTopicBytes);

            parsedWillMessage = new byte[p.WillMessageBytes.Length];
            p.WillMessageBytes.CopyTo(parsedWillMessage);

            var userBytes = new byte[p.UsernameUtf8Bytes.Length];
            p.UsernameUtf8Bytes.CopyTo(userBytes);
            parsedUsername = System.Text.Encoding.UTF8.GetString(userBytes);

            parsedPassword = new byte[p.PasswordBytes.Length];
            p.PasswordBytes.CopyTo(parsedPassword);

            parsedSessionExpiry = p.SessionExpiryInterval;
            parsedTopicAliasMax = p.TopicAliasMaximum;
            parsedMaxPacketSize = p.MaximumPacketSize;
            parsedRequestResponseInfo = p.RequestResponseInfo;
            parsedRequestProblemInfo = p.RequestProblemInfo;

            var authMethodBytes = new byte[p.AuthenticationMethodUtf8Bytes.Length];
            p.AuthenticationMethodUtf8Bytes.CopyTo(authMethodBytes);
            parsedAuthMethod = System.Text.Encoding.UTF8.GetString(authMethodBytes);

            parsedAuthData = new byte[p.AuthenticationDataBytes.Length];
            p.AuthenticationDataBytes.CopyTo(parsedAuthData);

            parsedWillDelay = p.WillDelayInterval;
            parsedWillPayloadFormat = p.WillPayloadFormatIndicator;
            parsedWillExpiry = p.WillMessageExpiryInterval;

            var contentTypeBytes = new byte[p.WillContentTypeUtf8Bytes.Length];
            p.WillContentTypeUtf8Bytes.CopyTo(contentTypeBytes);
            parsedWillContentType = System.Text.Encoding.UTF8.GetString(contentTypeBytes);

            var respTopicBytes = new byte[p.WillResponseTopicUtf8Bytes.Length];
            p.WillResponseTopicUtf8Bytes.CopyTo(respTopicBytes);
            parsedWillRespTopic = System.Text.Encoding.UTF8.GetString(respTopicBytes);

            parsedWillCorrData = new byte[p.WillCorrelationDataBytes.Length];
            p.WillCorrelationDataBytes.CopyTo(parsedWillCorrData);

            var propertiesEnumerator = p.GetProperties();
            while (propertiesEnumerator.MoveNext())
            {
               var prop = propertiesEnumerator.Current;
               if (prop.Identifier == PropertyIdentifier.UserProperty)
               {
                  var pair = prop.AsUserProperty();
                  var keyBytes = new byte[pair.KeyBytes.Length];
                  pair.KeyBytes.CopyTo(keyBytes);
                  var key = System.Text.Encoding.UTF8.GetString(keyBytes);

                  var valBytes = new byte[pair.ValueBytes.Length];
                  pair.ValueBytes.CopyTo(valBytes);
                  var val = System.Text.Encoding.UTF8.GetString(valBytes);

                  if (key == "conn-key" && val == "conn-val")
                  {
                     hasUserProp = true;
                  }
               }
            }

            var willPropertiesEnumerator = p.GetWillProperties();
            while (willPropertiesEnumerator.MoveNext())
            {
               var prop = willPropertiesEnumerator.Current;
               if (prop.Identifier == PropertyIdentifier.UserProperty)
               {
                  var pair = prop.AsUserProperty();
                  var keyBytes = new byte[pair.KeyBytes.Length];
                  pair.KeyBytes.CopyTo(keyBytes);
                  var key = System.Text.Encoding.UTF8.GetString(keyBytes);

                  var valBytes = new byte[pair.ValueBytes.Length];
                  pair.ValueBytes.CopyTo(valBytes);
                  var val = System.Text.Encoding.UTF8.GetString(valBytes);

                  if (key == "will-key" && val == "will-val")
                  {
                     hasWillUserProp = true;
                  }
               }
            }
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var options = new ConnectOptions
         {
            EndPoint = new System.Net.IPEndPoint(System.Net.IPAddress.Loopback, 1883),
            CleanSession = expectedCleanSession,
            KeepAlivePeriod = expectedKeepAlive,
            ClientIdUtf8Bytes = System.Text.Encoding.UTF8.GetBytes(expectedClientId),
            UsernameUtf8Bytes = System.Text.Encoding.UTF8.GetBytes(expectedUsername),
            PasswordBytes = expectedPassword,
            SessionExpiryInterval = expectedSessionExpiry,
            TopicAliasMaximum = expectedTopicAliasMax,
            MaximumPacketSize = expectedMaxPacketSize,
            RequestResponseInformation = expectedRequestResponseInfo,
            RequestProblemInformation = expectedRequestProblemInfo,
            AuthenticationMethodUtf8Bytes = System.Text.Encoding.UTF8.GetBytes(expectedAuthMethod),
            AuthenticationDataBytes = expectedAuthData,
            HasWill = expectedHasWill,
            WillQualityOfService = expectedWillQos,
            WillRetain = expectedWillRetain,
            WillTopicUtf8Bytes = System.Text.Encoding.UTF8.GetBytes(expectedWillTopic),
            WillPayload = new ReadOnlySequence<byte>(expectedWillMessage),
            WillDelayInterval = expectedWillDelay,
            WillPayloadFormatIndicator = expectedWillPayloadFormat,
            WillMessageExpiryInterval = expectedWillExpiry,
            WillContentTypeUtf8Bytes = System.Text.Encoding.UTF8.GetBytes(expectedWillContentType),
            WillResponseTopicUtf8Bytes = System.Text.Encoding.UTF8.GetBytes(expectedWillRespTopic),
            WillCorrelationDataBytes = expectedWillCorrData
         };
         options.UserProperties.Add("conn-key", "conn-val");
         options.WillUserProperties.Add("will-key", "will-val");

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteConnect(options);

         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.Unknown);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedCleanSession).IsEqualTo(expectedCleanSession);
      await Assert.That(parsedKeepAlive).IsEqualTo(expectedKeepAlive);
      await Assert.That(parsedClientId).IsEqualTo(expectedClientId);
      await Assert.That(parsedHasWill).IsEqualTo(expectedHasWill);
      await Assert.That(parsedWillQos).IsEqualTo(expectedWillQos);
      await Assert.That(parsedWillRetain).IsEqualTo(expectedWillRetain);
      await Assert.That(parsedWillTopic).IsEqualTo(expectedWillTopic);
      await Assert.That(parsedWillMessage).IsEquivalentTo(expectedWillMessage);
      await Assert.That(parsedUsername).IsEqualTo(expectedUsername);
      await Assert.That(parsedPassword).IsEquivalentTo(expectedPassword);

      await Assert.That(parsedSessionExpiry).IsEqualTo(expectedSessionExpiry);
      await Assert.That(parsedTopicAliasMax).IsEqualTo(expectedTopicAliasMax);
      await Assert.That(parsedMaxPacketSize).IsEqualTo(expectedMaxPacketSize);
      await Assert.That(parsedRequestResponseInfo).IsEqualTo(expectedRequestResponseInfo);
      await Assert.That(parsedRequestProblemInfo).IsEqualTo(expectedRequestProblemInfo);
      await Assert.That(parsedAuthMethod).IsEqualTo(expectedAuthMethod);
      await Assert.That(parsedAuthData).IsEquivalentTo(expectedAuthData);

      await Assert.That(parsedWillDelay).IsEqualTo(expectedWillDelay);
      await Assert.That(parsedWillPayloadFormat).IsEqualTo(expectedWillPayloadFormat);
      await Assert.That(parsedWillExpiry).IsEqualTo(expectedWillExpiry);
      await Assert.That(parsedWillContentType).IsEqualTo(expectedWillContentType);
      await Assert.That(parsedWillRespTopic).IsEqualTo(expectedWillRespTopic);
      await Assert.That(parsedWillCorrData).IsEquivalentTo(expectedWillCorrData);

      await Assert.That(hasUserProp).IsTrue();
      await Assert.That(hasWillUserProp).IsTrue();
   }
}
