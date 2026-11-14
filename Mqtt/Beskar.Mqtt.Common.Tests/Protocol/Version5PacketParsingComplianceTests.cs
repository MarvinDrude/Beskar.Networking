using System.Buffers;
using System.Text;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Encoders;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Common.Parsers;
using Beskar.Mqtt.Common.Tests.Helpers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Tests.Protocol;

public class Version5PacketParsingComplianceTests
{
   [Test]
   public async Task ParseConnectPacket_PasswordWithoutUsername_ShouldSucceed()
   {
      var buffer = new MemoryBuffer();
      var body = BuildConnectBody(
         connectFlags: 0x42, // CleanStart + Password, Username=0
         clientId: "pw-only",
         connectProperties: [],
         willTopic: null,
         willProperties: [],
         willPayload: [],
         password: "secret"u8);

      WritePacket(buffer, MqttPacketType.Connect, flags: 0, body);

      var connectInvoked = false;
      byte[]? parsedPassword = null;
      var handler = new TestPacketHandler
      {
         OnConnect = (in p) =>
         {
            connectInvoked = true;
            parsedPassword = p.PasswordBytes.ToArray();
         }
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.Unknown);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var result = await parser.TryDispatch(ref reader, out _);

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(connectInvoked).IsTrue();
      await Assert.That(parsedPassword).IsEquivalentTo("secret"u8.ToArray());
   }

   [Test]
   public async Task ParseConnectPacket_DuplicatePropertyAfterAllSingleConnectPropertiesSet_ShouldFail()
   {
      var propBuf = new byte[128];
      var pw = new ByteWriter(propBuf);
      var pe = pw.AsConnectPropertyEncoder();
      pe.WriteSessionExpiryInterval(10);
      pe.WriteReceiveMaximum(20);
      pe.WriteMaximumPacketSize(4096);
      pe.WriteTopicAliasMaximum(5);
      pe.WriteRequestResponseInformation(true);
      pe.WriteRequestProblemInformation(true);
      pe.WriteAuthenticationMethod("SCRAM"u8);
      pe.WriteAuthenticationData([1, 2]);
      // Duplicate SessionExpiryInterval after all 8 flags in foundFlags are set
      pe.WriteSessionExpiryInterval(20);
      pw = pe.Encoder.Writer;

      var body = BuildConnectBody(
         connectFlags: 0x02,
         clientId: "dup-conn-prop",
         connectProperties: propBuf[..pw.Position],
         willTopic: null,
         willProperties: [],
         willPayload: [],
         password: []);

      var buffer = new MemoryBuffer();
      WritePacket(buffer, MqttPacketType.Connect, flags: 0, body);

      var connectInvoked = false;
      var handler = new TestPacketHandler { OnConnect = (in _) => connectInvoked = true };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.Unknown);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var result = await parser.TryDispatch(ref reader, out _);

      await Assert.That(result.Failed || result.Success != PacketDispatchResult.Success).IsTrue();
      await Assert.That(connectInvoked).IsFalse();
   }

   [Test]
   public async Task ParseConnectPacket_DuplicateWillPropertyAfterAllSingleWillPropertiesSet_ShouldFail()
   {
      var willPropBuf = new byte[128];
      var wp = new ByteWriter(willPropBuf);
      var wpe = wp.AsWillPropertyEncoder();
      wpe.WriteWillDelayInterval(5);
      wpe.WritePayloadFormatIndicator(PayloadFormat.CharacterData);
      wpe.WriteMessageExpiryInterval(30);
      wpe.WriteContentType("text/plain"u8);
      wpe.WriteResponseTopic("will/resp"u8);
      wpe.WriteCorrelationData([9, 8]);
      // Duplicate WillDelayInterval after all 6 flags in foundFlags are set
      wpe.WriteWillDelayInterval(10);
      wp = wpe.Encoder.Writer;

      var body = BuildConnectBody(
         connectFlags: 0x06, // CleanStart + WillFlag
         clientId: "dup-will-prop",
         connectProperties: [],
         willTopic: "will/topic",
         willProperties: willPropBuf[..wp.Position],
         willPayload: "offline"u8,
         password: []);

      var buffer = new MemoryBuffer();
      WritePacket(buffer, MqttPacketType.Connect, flags: 0, body);

      var connectInvoked = false;
      var handler = new TestPacketHandler { OnConnect = (in _) => connectInvoked = true };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.Unknown);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var result = await parser.TryDispatch(ref reader, out _);

      await Assert.That(result.Failed || result.Success != PacketDispatchResult.Success).IsTrue();
      await Assert.That(connectInvoked).IsFalse();
   }

   [Test]
   public async Task ParseSubscribePacket_DuplicateSubscriptionIdentifier_ShouldFail()
   {
      var body = new byte[64];
      var writer = new ByteWriter(body);
      writer.WriteBigEndian((ushort)1); // Packet Identifier

      // Properties: len = 4 -> SubscriptionIdentifier(0x0B) = 1, SubscriptionIdentifier(0x0B) = 2
      writer.WriteByte(4);
      writer.WriteByte((byte)PropertyIdentifier.SubscriptionIdentifier);
      writer.WriteByte(1);
      writer.WriteByte((byte)PropertyIdentifier.SubscriptionIdentifier);
      writer.WriteByte(2);

      // Filter: "a/b", QoS 0
      writer.WriteBigEndian((ushort)3);
      writer.WriteBytes("a/b"u8);
      writer.WriteByte(0x00);

      var buffer = new MemoryBuffer();
      WritePacket(buffer, MqttPacketType.Subscribe, flags: 0x02, body.AsSpan(0, writer.Position));

      var subscribeInvoked = false;
      var handler = new TestPacketHandler { OnSubscribe = (in _) => subscribeInvoked = true };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var result = await parser.TryDispatch(ref reader, out _);

      await Assert.That(result.Failed || result.Success != PacketDispatchResult.Success).IsTrue();
      await Assert.That(subscribeInvoked).IsFalse();
   }

   [Test]
   public async Task ParseUnsubscribePacket_InvalidPropertyIdentifier_ShouldFail()
   {
      var body = new byte[64];
      var writer = new ByteWriter(body);
      writer.WriteBigEndian((ushort)1); // Packet Identifier

      // Invalid property in UNSUBSCRIBE: SessionExpiryInterval (0x11) = 10 (only UserProperty 0x26 is valid)
      writer.WriteByte(5);
      writer.WriteByte((byte)PropertyIdentifier.SessionExpiryInterval);
      writer.WriteBigEndian(10U);

      // Filter: "a/b"
      writer.WriteBigEndian((ushort)3);
      writer.WriteBytes("a/b"u8);

      var buffer = new MemoryBuffer();
      WritePacket(buffer, MqttPacketType.Unsubscribe, flags: 0x02, body.AsSpan(0, writer.Position));

      var unsubscribeInvoked = false;
      var handler = new TestPacketHandler { OnUnsubscribe = (in _) => unsubscribeInvoked = true };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var result = await parser.TryDispatch(ref reader, out _);

      await Assert.That(result.Failed || result.Success != PacketDispatchResult.Success).IsTrue();
      await Assert.That(unsubscribeInvoked).IsFalse();
   }

   [Test]
   public async Task ParseUnsubscribePacket_MalformedOrTruncatedProperty_ShouldFail()
   {
      var body = new byte[64];
      var writer = new ByteWriter(body);
      writer.WriteBigEndian((ushort)2); // Packet Identifier

      // Property length = 1, unknown/truncated property identifier 0x7F
      writer.WriteByte(1);
      writer.WriteByte(0x7F);

      // Valid filter: "a/b"
      writer.WriteBigEndian((ushort)3);
      writer.WriteBytes("a/b"u8);

      var buffer = new MemoryBuffer();
      WritePacket(buffer, MqttPacketType.Unsubscribe, flags: 0x02, body.AsSpan(0, writer.Position));

      var unsubscribeInvoked = false;
      var handler = new TestPacketHandler { OnUnsubscribe = (in _) => unsubscribeInvoked = true };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var result = await parser.TryDispatch(ref reader, out _);

      await Assert.That(result.Failed || result.Success != PacketDispatchResult.Success).IsTrue();
      await Assert.That(unsubscribeInvoked).IsFalse();
   }

   [Test]
   public async Task ParseSubAckPacket_DuplicateOrMalformedPropertyAfterReasonString_ShouldFail()
   {
      var body = new byte[64];
      var writer = new ByteWriter(body);
      writer.WriteBigEndian((ushort)3); // Packet Identifier

      // Properties: ReasonString "ok" (1 + 2 + 2 = 5 bytes), followed by unknown/truncated byte 0x7F (total 6 bytes)
      writer.WriteByte(6);
      writer.WriteByte((byte)PropertyIdentifier.ReasonString);
      writer.WriteBigEndian((ushort)2);
      writer.WriteBytes("ok"u8);
      writer.WriteByte(0x7F);

      writer.WriteByte(0x00); // Granted QoS 0

      var buffer = new MemoryBuffer();
      WritePacket(buffer, MqttPacketType.SubAck, flags: 0x00, body.AsSpan(0, writer.Position));

      var subAckInvoked = false;
      var handler = new TestPacketHandler { OnSubAck = (in _) => subAckInvoked = true };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var reader = new SequenceReader<byte>(buffer.WrittenSequence);
      var result = await parser.TryDispatch(ref reader, out _);

      await Assert.That(result.Failed || result.Success != PacketDispatchResult.Success).IsTrue();
      await Assert.That(subAckInvoked).IsFalse();
   }

   private static void WritePacket(MemoryBuffer buffer, MqttPacketType type, byte flags, ReadOnlySpan<byte> body)
   {
      var total = new byte[5 + body.Length];
      var writer = new ByteWriter(total);
      PacketEncoder.WriteFixedHeader(ref writer, type, flags, body.Length);
      writer.WriteBytes(body);
      buffer.Write(total.AsSpan(0, writer.Position));
   }

   private static byte[] BuildConnectBody(
      byte connectFlags,
      string clientId,
      ReadOnlySpan<byte> connectProperties,
      string? willTopic,
      ReadOnlySpan<byte> willProperties,
      ReadOnlySpan<byte> willPayload,
      ReadOnlySpan<byte> password)
   {
      var body = new byte[512];
      var writer = new ByteWriter(body);

      writer.WriteBigEndian((ushort)4);
      writer.WriteBytes("MQTT"u8);
      writer.WriteByte(5); // V5.0
      writer.WriteByte(connectFlags);
      writer.WriteBigEndian((ushort)60);

      PacketEncoder.WriteVariableByteInteger(ref writer, (uint)connectProperties.Length);
      if (!connectProperties.IsEmpty)
      {
         writer.WriteBytes(connectProperties);
      }

      var clientIdBytes = Encoding.UTF8.GetBytes(clientId);
      writer.WriteBigEndian((ushort)clientIdBytes.Length);
      writer.WriteBytes(clientIdBytes);

      if ((connectFlags & 0x04) != 0)
      {
         PacketEncoder.WriteVariableByteInteger(ref writer, (uint)willProperties.Length);
         if (!willProperties.IsEmpty)
         {
            writer.WriteBytes(willProperties);
         }

         var willTopicBytes = Encoding.UTF8.GetBytes(willTopic ?? string.Empty);
         writer.WriteBigEndian((ushort)willTopicBytes.Length);
         writer.WriteBytes(willTopicBytes);

         writer.WriteBigEndian((ushort)willPayload.Length);
         if (!willPayload.IsEmpty)
         {
            writer.WriteBytes(willPayload);
         }
      }

      if ((connectFlags & 0x40) != 0)
      {
         writer.WriteBigEndian((ushort)password.Length);
         if (!password.IsEmpty)
         {
            writer.WriteBytes(password);
         }
      }

      return body[..writer.Position];
   }
}
