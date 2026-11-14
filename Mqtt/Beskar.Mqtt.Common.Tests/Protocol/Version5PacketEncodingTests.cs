using System.Buffers;
using System.Text;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Encoders;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Common.Parsers;
using Beskar.Mqtt.Common.Tests.Helpers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Tests.Protocol;

public class Version5PacketEncodingTests
{
   [Test]
   public async Task WriteConnect_WithTypedConnectAndWillProperties_ShouldEncodeProperties()
   {
      var buffer = new MemoryBuffer();
      var packet = new ConnectPacket
      {
         IsCleanSession = true,
         KeepAliveInterval = 60,
         ClientIdUtf8Bytes = new ReadOnlySequence<byte>("typed-connect"u8.ToArray()),
         SessionExpiryInterval = 120,
         ReceiveMaximum = 15,
         TopicAliasMaximum = 8,
         HasWill = true,
         WillTopicUtf8Bytes = new ReadOnlySequence<byte>("will/topic"u8.ToArray()),
         WillMessageBytes = new ReadOnlySequence<byte>("bye"u8.ToArray()),
         WillQualityOfService = QualityOfServiceType.AtLeastOnce,
         WillDelayInterval = 30,
         WillMessageExpiryInterval = 45,
         PropertiesBytes = ReadOnlySequence<byte>.Empty,
         WillPropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      var encoder = new PacketVersion5Encoder(buffer);
      encoder.WriteConnect(in packet);

      ConnectPacket? parsed = null;
      var handler = new TestPacketHandler
      {
         OnConnect = (in p) => parsed = CopyConnect(in p)
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.Unknown);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var dispatchResult = await parser.TryDispatch(ref reader, out _);

      await Assert.That(dispatchResult.Failed).IsFalse();
      await Assert.That(dispatchResult.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(parsed.HasValue).IsTrue();
      await Assert.That(parsed!.Value.SessionExpiryInterval).IsEqualTo(120U);
      await Assert.That(parsed!.Value.ReceiveMaximum).IsEqualTo((ushort)15);
      await Assert.That(parsed!.Value.TopicAliasMaximum).IsEqualTo((ushort)8);
      await Assert.That(parsed!.Value.WillDelayInterval).IsEqualTo(30U);
      await Assert.That(parsed!.Value.WillMessageExpiryInterval).IsEqualTo(45U);
   }

   [Test]
   public async Task WritePublish_WithTypedProperties_ShouldEncodeProperties()
   {
      var buffer = new MemoryBuffer();
      var packet = new PublishPacket
      {
         TopicUtf8Bytes = new ReadOnlySequence<byte>("typed/publish"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("payload"u8.ToArray()),
         QualityOfService = QualityOfServiceType.AtMostOnce,
         PayloadFormat = PayloadFormat.CharacterData,
         MessageExpiryInterval = 90,
         TopicAlias = 4,
         ResponseTopicUtf8Bytes = new ReadOnlySequence<byte>("resp/topic"u8.ToArray()),
         CorrelationDataBytes = new ReadOnlySequence<byte>([1, 2, 3]),
         ContentTypeUtf8Bytes = new ReadOnlySequence<byte>("text/plain"u8.ToArray()),
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      var encoder = new PacketVersion5Encoder(buffer);
      encoder.WritePublish(in packet);

      uint parsedExpiry = 0;
      ushort parsedAlias = 0;
      PayloadFormat parsedFormat = PayloadFormat.Unspecified;
      string parsedResponseTopic = string.Empty;
      string parsedContentType = string.Empty;

      var handler = new TestPacketHandler
      {
         OnPublish = (in p) =>
         {
            parsedExpiry = p.MessageExpiryInterval;
            parsedAlias = p.TopicAlias;
            parsedFormat = p.PayloadFormat;
            parsedResponseTopic = Encoding.UTF8.GetString(p.ResponseTopicUtf8Bytes);
            parsedContentType = Encoding.UTF8.GetString(p.ContentTypeUtf8Bytes);
         }
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var dispatchResult = await parser.TryDispatch(ref reader, out _);

      await Assert.That(dispatchResult.Failed).IsFalse();
      await Assert.That(dispatchResult.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(parsedFormat).IsEqualTo(PayloadFormat.CharacterData);
      await Assert.That(parsedExpiry).IsEqualTo(90U);
      await Assert.That(parsedAlias).IsEqualTo((ushort)4);
      await Assert.That(parsedResponseTopic).IsEqualTo("resp/topic");
      await Assert.That(parsedContentType).IsEqualTo("text/plain");
   }

   [Test]
   public async Task WriteConnAck_WithTypedProperties_ShouldEncodeProperties()
   {
      var buffer = new MemoryBuffer();
      var packet = new ConnAckPacket
      {
         IsSessionPresent = false,
         ReasonCode = ConnectReasonCode.Success,
         SessionExpiryInterval = 60,
         ReceiveMaximum = 25,
         TopicAliasMaximum = 12,
         ServerKeepAlive = 30,
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      var encoder = new PacketVersion5Encoder(buffer);
      encoder.WriteConnAck(in packet);

      uint parsedExpiry = 0;
      ushort parsedRecvMax = 0;
      ushort parsedAliasMax = 0;
      ushort parsedKeepAlive = 0;

      var handler = new TestPacketHandler
      {
         OnConnAck = (in p) =>
         {
            parsedExpiry = p.SessionExpiryInterval;
            parsedRecvMax = p.ReceiveMaximum;
            parsedAliasMax = p.TopicAliasMaximum;
            parsedKeepAlive = p.ServerKeepAlive;
         }
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var dispatchResult = await parser.TryDispatch(ref reader, out _);

      await Assert.That(dispatchResult.Failed).IsFalse();
      await Assert.That(dispatchResult.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(parsedExpiry).IsEqualTo(60U);
      await Assert.That(parsedRecvMax).IsEqualTo((ushort)25);
      await Assert.That(parsedAliasMax).IsEqualTo((ushort)12);
      await Assert.That(parsedKeepAlive).IsEqualTo((ushort)30);
   }

   [Test]
   public async Task WriteSubscribe_WithTypedSubscriptionIdentifier_ShouldEncodeProperty()
   {
      var buffer = new MemoryBuffer();
      var filterBytes = BuildSubscribeFilter("typed/sub", optionsByte: 0x01);

      var packet = new SubscribePacket
      {
         PacketIdentifier = 7,
         SubscriptionIdentifier = 42,
         FiltersBytes = new ReadOnlySequence<byte>(filterBytes),
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      var encoder = new PacketVersion5Encoder(buffer);
      encoder.WriteSubscribe(in packet);

      uint parsedSubId = 0;
      var handler = new TestPacketHandler
      {
         OnSubscribe = (in p) => parsedSubId = p.SubscriptionIdentifier
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var dispatchResult = await parser.TryDispatch(ref reader, out _);

      await Assert.That(dispatchResult.Failed).IsFalse();
      await Assert.That(dispatchResult.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(parsedSubId).IsEqualTo(42U);
   }

   [Test]
   public async Task WriteDisconnect_WithTypedProperties_ShouldEncodeProperties()
   {
      var buffer = new MemoryBuffer();
      var packet = new DisconnectPacket
      {
         ReasonCode = DisconnectReasonCode.NormalDisconnection,
         SessionExpiryInterval = 300,
         ServerReferenceUtf8Bytes = new ReadOnlySequence<byte>("backup.local"u8.ToArray()),
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      var encoder = new PacketVersion5Encoder(buffer);
      encoder.WriteDisconnect(in packet);

      uint? parsedExpiry = null;
      string parsedServerRef = string.Empty;
      var handler = new TestPacketHandler
      {
         OnDisconnect = (in p) =>
         {
            parsedExpiry = p.SessionExpiryInterval;
            parsedServerRef = Encoding.UTF8.GetString(p.ServerReferenceUtf8Bytes);
         }
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var dispatchResult = await parser.TryDispatch(ref reader, out _);

      await Assert.That(dispatchResult.Failed).IsFalse();
      await Assert.That(dispatchResult.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(parsedExpiry).IsEqualTo(300U);
      await Assert.That(parsedServerRef).IsEqualTo("backup.local");
   }

   [Test]
   public async Task WriteAuth_FromParsedPacket_ShouldNotDuplicateProperties()
   {
      var props = new byte[64];
      var pw = new ByteWriter(props);
      var ape = pw.AsAuthPropertyEncoder();
      ape.WriteAuthenticationMethod("SCRAM-SHA-256"u8);
      ape.WriteAuthenticationData([0xAA, 0xBB]);
      ape.WriteReasonString("step1"u8);
      pw = ape.Encoder.Writer;

      var raw = new byte[128];
      var rw = new ByteWriter(raw);
      PacketEncoder.WriteFixedHeader(ref rw, MqttPacketType.Auth, 0, 1 + 1 + pw.Position);
      rw.WriteByte((byte)AuthenticateReasonCode.ContinueAuthentication);
      rw.WriteByte((byte)pw.Position);
      rw.WriteBytes(props.AsSpan(0, pw.Position));

      AuthPacket parsed = default;
      var parsedHandler = new TestPacketHandler
      {
         OnAuth = (in p) =>
         {
            parsed = new AuthPacket
            {
               ReasonCode = p.ReasonCode,
               AuthenticationMethodUtf8Bytes = new ReadOnlySequence<byte>(p.AuthenticationMethodUtf8Bytes.ToArray()),
               AuthenticationDataBytes = new ReadOnlySequence<byte>(p.AuthenticationDataBytes.ToArray()),
               ReasonUtf8Bytes = new ReadOnlySequence<byte>(p.ReasonUtf8Bytes.ToArray()),
               PropertiesBytes = new ReadOnlySequence<byte>(p.PropertiesBytes.ToArray())
            };
         }
      };

      var parser1 = new PacketParser(new DummyNetworkStream(), parsedHandler, MqttProtocolVersion.V50);
      var reader1 = new SequenceReader<byte>(new ReadOnlySequence<byte>(raw[..rw.Position]));
      var res1 = await parser1.TryDispatch(ref reader1, out _);
      await Assert.That(res1.Failed).IsFalse();
      await Assert.That(res1.Success).IsEqualTo(PacketDispatchResult.Success);

      // Re-encode the parsed AUTH packet (which contains both typed fields and PropertiesBytes)
      var reencodedBuffer = new MemoryBuffer();
      new PacketVersion5Encoder(reencodedBuffer).WriteAuth(in parsed);

      var authMethodCount = 0;
      var countHandler = new TestPacketHandler
      {
         OnAuth = (in p) =>
         {
            var enumerator = p.GetProperties();
            while (enumerator.MoveNext())
            {
               if (enumerator.Current.Identifier == PropertyIdentifier.AuthenticationMethod)
               {
                  authMethodCount++;
               }
            }
         }
      };

      var parser2 = new PacketParser(new DummyNetworkStream(), countHandler, MqttProtocolVersion.V50);
      var reader2 = new SequenceReader<byte>(reencodedBuffer.WrittenSequence);
      var res2 = await parser2.TryDispatch(ref reader2, out _);
      await Assert.That(res2.Failed).IsFalse();
      await Assert.That(authMethodCount).IsEqualTo(1);
   }

   [Test]
   public async Task WriteServerKeepAlive_ZeroValue_ShouldWritePropertyBytes()
   {
      var buffer = new byte[16];
      var writer = new ByteWriter(buffer);
      var propEncoder = writer.AsConnAckPropertyEncoder();

      propEncoder.WriteServerKeepAlive(0);
      writer = propEncoder.Encoder.Writer;

      await Assert.That(writer.Position).IsEqualTo(3);
      await Assert.That(buffer[0]).IsEqualTo((byte)PropertyIdentifier.ServerKeepAlive);
      await Assert.That(buffer[1]).IsEqualTo((byte)0);
      await Assert.That(buffer[2]).IsEqualTo((byte)0);
   }

   [Test]
   public async Task WriteMessageExpiryInterval_ZeroValue_ShouldWritePropertyBytes()
   {
      var buffer = new byte[16];
      var writer = new ByteWriter(buffer);
      var propEncoder = writer.AsPublishPropertyEncoder();

      propEncoder.WriteMessageExpiryInterval(0);
      writer = propEncoder.Encoder.Writer;

      await Assert.That(writer.Position).IsEqualTo(5);
      await Assert.That(buffer[0]).IsEqualTo((byte)PropertyIdentifier.MessageExpiryInterval);
      await Assert.That(buffer[1]).IsEqualTo((byte)0);
      await Assert.That(buffer[2]).IsEqualTo((byte)0);
      await Assert.That(buffer[3]).IsEqualTo((byte)0);
      await Assert.That(buffer[4]).IsEqualTo((byte)0);
   }

   private static byte[] BuildSubscribeFilter(string topic, byte optionsByte)
   {
      var topicBytes = Encoding.UTF8.GetBytes(topic);
      var result = new byte[2 + topicBytes.Length + 1];
      var writer = new ByteWriter(result);
      writer.WriteBigEndian((ushort)topicBytes.Length);
      writer.WriteBytes(topicBytes);
      writer.WriteByte(optionsByte);
      return result;
   }

   private static ConnectPacket CopyConnect(in ConnectPacket p) => new()
   {
      IsCleanSession = p.IsCleanSession,
      KeepAliveInterval = p.KeepAliveInterval,
      ClientIdUtf8Bytes = new ReadOnlySequence<byte>(p.ClientIdUtf8Bytes.ToArray()),
      SessionExpiryInterval = p.SessionExpiryInterval,
      ReceiveMaximum = p.ReceiveMaximum,
      TopicAliasMaximum = p.TopicAliasMaximum,
      HasWill = p.HasWill,
      WillDelayInterval = p.WillDelayInterval,
      WillMessageExpiryInterval = p.WillMessageExpiryInterval
   };
}
