using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Mqtt.Common.Encoders.Version3;
using Beskar.Mqtt.Common.Parsers;
using Beskar.Mqtt.Common.Tests.Helpers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Tests.Protocol;

public class Version3ParsingTests
{
   [Test]
   public async Task CorrectPubRecParsing()
   {
      // Arrange
      var buffer = new MemoryBuffer();
      const ushort originalPacketId = 20;
      var wasInvoked = false;
      ushort parsedPacketId = 0;

      var handler = new TestPacketHandler
      {
         OnPubRec = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new PubRecPacket
         {
            PacketIdentifier = originalPacketId
         };

         var encoder = new PacketVersion3Encoder(buffer, MqttProtocolVersion.V311);
         encoder.WritePubRec(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V311);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      // Assert
      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
   }

   [Test]
   public async Task CorrectPubAckParsing()
   {
      var buffer = new MemoryBuffer();
      const ushort originalPacketId = 42;
      var wasInvoked = false;
      ushort parsedPacketId = 0;

      var handler = new TestPacketHandler
      {
         OnPubAck = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new PubAckPacket
         {
            PacketIdentifier = originalPacketId
         };

         var encoder = new PacketVersion3Encoder(buffer, MqttProtocolVersion.V311);
         encoder.WritePubAck(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V311);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
   }

   [Test]
   public async Task CorrectPubCompParsing()
   {
      var buffer = new MemoryBuffer();
      const ushort originalPacketId = 43;
      var wasInvoked = false;
      ushort parsedPacketId = 0;

      var handler = new TestPacketHandler
      {
         OnPubComp = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new PubCompPacket
         {
            PacketIdentifier = originalPacketId
         };

         var encoder = new PacketVersion3Encoder(buffer, MqttProtocolVersion.V311);
         encoder.WritePubComp(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V311);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
   }

   [Test]
   public async Task CorrectPubRelParsing()
   {
      var buffer = new MemoryBuffer();
      const ushort originalPacketId = 44;
      var wasInvoked = false;
      ushort parsedPacketId = 0;

      var handler = new TestPacketHandler
      {
         OnPubRel = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new PubRelPacket
         {
            PacketIdentifier = originalPacketId
         };

         var encoder = new PacketVersion3Encoder(buffer, MqttProtocolVersion.V311);
         encoder.WritePubRel(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V311);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
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

      var parsedDup = false;
      var parsedQos = QualityOfServiceType.AtMostOnce;
      var parsedRetain = false;
      ushort parsedPacketId = 0;
      string? parsedTopic = null;
      byte[]? parsedPayload = null;

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
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new PublishPacket
         {
            Dup = expectedDup,
            QualityOfService = expectedQos,
            Retain = expectedRetain,
            PacketIdentifier = expectedPacketId,
            TopicUtf8Bytes = new ReadOnlySequence<byte>(System.Text.Encoding.UTF8.GetBytes(expectedTopic)),
            Payload = new ReadOnlySequence<byte>(expectedPayload)
         };

         var encoder = new PacketVersion3Encoder(buffer, MqttProtocolVersion.V311);
         encoder.WritePublish(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V311);
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
   }

   [Test]
   public async Task CorrectConnectParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      var expectedCleanSession = true;
      var expectedKeepAlive = (ushort)60;
      var expectedClientId = "client-123";
      var expectedHasWill = true;
      var expectedWillQos = QualityOfServiceType.ExactlyOnce;
      var expectedWillRetain = true;
      var expectedWillTopic = "will/topic";
      var expectedWillMessage = new byte[] { 1, 3, 5 };
      var expectedUsername = "admin";
      var expectedPassword = new byte[] { 2, 4, 6 };

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
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new ConnectPacket
         {
            IsCleanSession = expectedCleanSession,
            KeepAliveInterval = expectedKeepAlive,
            ClientIdUtf8Bytes = new ReadOnlySequence<byte>(System.Text.Encoding.UTF8.GetBytes(expectedClientId)),
            HasWill = expectedHasWill,
            WillQualityOfService = expectedWillQos,
            WillRetain = expectedWillRetain,
            WillTopicUtf8Bytes = new ReadOnlySequence<byte>(System.Text.Encoding.UTF8.GetBytes(expectedWillTopic)),
            WillMessageBytes = new ReadOnlySequence<byte>(expectedWillMessage),
            UsernameUtf8Bytes = new ReadOnlySequence<byte>(System.Text.Encoding.UTF8.GetBytes(expectedUsername)),
            PasswordBytes = new ReadOnlySequence<byte>(expectedPassword)
         };

         var encoder = new PacketVersion3Encoder(buffer, MqttProtocolVersion.V311);
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
      await Assert.That(parsedHasWill).IsEqualTo(expectedHasWill);
      await Assert.That(parsedWillQos).IsEqualTo(expectedWillQos);
      await Assert.That(parsedWillRetain).IsEqualTo(expectedWillRetain);
      await Assert.That(parsedWillTopic).IsEqualTo(expectedWillTopic);
      await Assert.That(parsedWillMessage).IsEquivalentTo(expectedWillMessage);
      await Assert.That(parsedUsername).IsEqualTo(expectedUsername);
      await Assert.That(parsedPassword).IsEquivalentTo(expectedPassword);
   }

   [Test]
   public async Task CorrectConnAckParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      var expectedSessionPresent = true;
      var expectedReturnCode = ConnectReturnCode.IdentifierRejected;

      var parsedSessionPresent = false;
      var parsedReturnCode = ConnectReturnCode.Accepted;

      var handler = new TestPacketHandler
      {
         OnConnAck = (in p) =>
         {
            wasInvoked = true;
            parsedSessionPresent = p.IsSessionPresent;
            parsedReturnCode = p.ReturnCode;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new ConnAckPacket
         {
            IsSessionPresent = expectedSessionPresent,
            ReturnCode = expectedReturnCode
         };

         var encoder = new PacketVersion3Encoder(buffer, MqttProtocolVersion.V311);
         encoder.WriteConnAck(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V311);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedSessionPresent).IsEqualTo(expectedSessionPresent);
      await Assert.That(parsedReturnCode).IsEqualTo(expectedReturnCode);
   }

   [Test]
   public async Task CorrectSubAckParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      ushort expectedPacketId = 77;
      var expectedReturnCodes = new byte[] { 0x00, 0x01, 0x80 };

      ushort parsedPacketId = 0;
      byte[]? parsedReturnCodes = null;

      var handler = new TestPacketHandler
      {
         OnSubAck = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;

            parsedReturnCodes = new byte[p.ReturnCodesBytes.Length];
            p.ReturnCodesBytes.CopyTo(parsedReturnCodes);
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new SubAckPacket
         {
            PacketIdentifier = expectedPacketId,
            ReturnCodesBytes = new ReadOnlySequence<byte>(expectedReturnCodes)
         };

         var encoder = new PacketVersion3Encoder(buffer, MqttProtocolVersion.V311);
         encoder.WriteSubAck(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V311);
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
   }

   [Test]
   public async Task CorrectSubscribeParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      ushort expectedPacketId = 88;
      // SUBSCRIBE payload: topic "test" (length 4) + QoS 1
      var expectedFiltersBytes = new byte[] { 0x00, 0x04, (byte)'t', (byte)'e', (byte)'s', (byte)'t', 0x01 };

      ushort parsedPacketId = 0;
      byte[]? parsedFilters = null;

      var handler = new TestPacketHandler
      {
         OnSubscribe = (in p) =>
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
         var originalPacket = new SubscribePacket
         {
            PacketIdentifier = expectedPacketId,
            FiltersBytes = new ReadOnlySequence<byte>(expectedFiltersBytes)
         };

         var encoder = new PacketVersion3Encoder(buffer, MqttProtocolVersion.V311);
         encoder.WriteSubscribe(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V311);
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
   public async Task InvalidSubscribeParsingWithOptionsUnderV3()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      ushort expectedPacketId = 88;
      // SUBSCRIBE payload: topic "test" (length 4) + option byte with NL bit set (invalid for v3) -> 0x05 (QoS=1, NL=1)
      var expectedFiltersBytes = new byte[] { 0x00, 0x04, (byte)'t', (byte)'e', (byte)'s', (byte)'t', 0x05 };

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
            FiltersBytes = new ReadOnlySequence<byte>(expectedFiltersBytes)
         };

         var encoder = new PacketVersion3Encoder(buffer, MqttProtocolVersion.V311);
         encoder.WriteSubscribe(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V311);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.ProtocolError);
      await Assert.That(wasInvoked).IsFalse();
   }

   [Test]
   public async Task CorrectUnsubAckParsing()
   {
      var buffer = new MemoryBuffer();
      ushort originalPacketId = 99;
      var wasInvoked = false;
      ushort parsedPacketId = 0;

      var handler = new TestPacketHandler
      {
         OnUnsubAck = (in p) =>
         {
            wasInvoked = true;
            parsedPacketId = p.PacketIdentifier;
         }
      };

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;

      {
         var originalPacket = new UnsubAckPacket
         {
            PacketIdentifier = originalPacketId
         };

         var encoder = new PacketVersion3Encoder(buffer, MqttProtocolVersion.V311);
         encoder.WriteUnsubAck(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V311);
         var reader = new SequenceReader<byte>(buffer.WrittenSequence);
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(bytesConsumed).IsEqualTo(buffer.WrittenSpan.Length);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPacketId).IsEqualTo(originalPacketId);
   }

   [Test]
   public async Task CorrectUnsubscribeParsing()
   {
      var buffer = new MemoryBuffer();
      var wasInvoked = false;

      ushort expectedPacketId = 111;
      // UNSUBSCRIBE payload: topic "test" (length 4) without QoS
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
            FiltersBytes = new ReadOnlySequence<byte>(expectedFiltersBytes)
         };

         var encoder = new PacketVersion3Encoder(buffer, MqttProtocolVersion.V311);
         encoder.WriteUnsubscribe(originalPacket);

         var parser = new PacketParser(handler, MqttProtocolVersion.V311);
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
}
