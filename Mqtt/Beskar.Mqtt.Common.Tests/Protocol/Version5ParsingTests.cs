using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Memory.Writers;
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
            PropertiesBytes = propBuffer.WrittenSequence
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubRec(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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
            PropertiesBytes = ReadOnlySequence<byte>.Empty
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubAck(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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

         var parser = new PacketParser(handler, MqttProtocolVersion.Unknown);
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

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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
            PropertiesBytes = propBuffer.WrittenSequence
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubAck(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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
            PropertiesBytes = ReadOnlySequence<byte>.Empty
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubRec(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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
            PropertiesBytes = propBuffer.WrittenSequence
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubRel(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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
            PropertiesBytes = ReadOnlySequence<byte>.Empty
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubRel(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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
            PropertiesBytes = propBuffer.WrittenSequence
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubComp(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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
            PropertiesBytes = ReadOnlySequence<byte>.Empty
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WritePubComp(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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
            ReturnCodesBytes = new ReadOnlySequence<byte>(expectedReturnCodes),
            PropertiesBytes = propBuffer.WrittenSequence
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteSubAck(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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
            ReasonCodesBytes = new ReadOnlySequence<byte>(expectedReasonCodes),
            PropertiesBytes = propBuffer.WrittenSequence
         };

         var encoder = new PacketVersion5Encoder(buffer);
         encoder.WriteUnsubAck(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V50);
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
}
