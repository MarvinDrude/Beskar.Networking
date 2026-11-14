using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Encoders;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Common.Parsers;
using Beskar.Mqtt.Common.Tests.Helpers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Tests.Protocol;

public class PacketFramingTests
{
   [Test]
   public async Task TryDispatch_FragmentedPacketBody_ShouldNotAdvanceReaderPastHeader()
   {
      var buffer = new MemoryBuffer();
      var packet = new PublishPacket
      {
         TopicUtf8Bytes = new ReadOnlySequence<byte>("sensor/temperature"u8.ToArray()),
         Payload = new ReadOnlySequence<byte>("temperature-reading-value"u8.ToArray()),
         QualityOfService = QualityOfServiceType.AtMostOnce,
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      var encoder = new PacketVersion5Encoder(buffer);
      encoder.WritePublish(in packet);

      var fullBytes = buffer.WrittenSequence.ToArray();
      var fragmentedSlice = fullBytes[..5];

      var handler = new TestPacketHandler();

      ValueTask<Result<PacketDispatchResult, StringError>> dispatchTask;
      int bytesConsumed;
      long readerConsumed;
      long readerRemaining;
      {
         var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
         var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(fragmentedSlice));
         dispatchTask = parser.TryDispatch(ref reader, out bytesConsumed);
         readerConsumed = reader.Consumed;
         readerRemaining = reader.Remaining;
      }

      var result = await dispatchTask;

      await Assert.That(result.Failed).IsFalse();
      await Assert.That(result.Success).IsEqualTo(PacketDispatchResult.NotEnoughData);
      await Assert.That(bytesConsumed).IsEqualTo(0);
      await Assert.That(readerConsumed).IsEqualTo(0);
      await Assert.That(readerRemaining).IsEqualTo(fragmentedSlice.Length);
   }

   [Test]
   public async Task TryDispatch_ConnectWithVersion5AndHighBitSet_ShouldRejectProtocolVersion()
   {
      var packetBytes = BuildRawConnectPacket(protocolLevel: 0x85, clientId: "bridge-client");

      var connectInvoked = false;
      var handler = new TestPacketHandler
      {
         OnConnect = (in _) => connectInvoked = true
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.Unknown);
      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(packetBytes));
      var result = await parser.TryDispatch(ref reader, out _);

      await Assert.That(result.Failed || result.Success != PacketDispatchResult.Success).IsTrue();
      await Assert.That(connectInvoked).IsFalse();
   }

   private static byte[] BuildRawConnectPacket(byte protocolLevel, string clientId)
   {
      var clientIdBytes = System.Text.Encoding.UTF8.GetBytes(clientId);
      var bodyLength = 6 + 1 + 1 + 2 + 1 + 2 + clientIdBytes.Length;
      var total = new byte[2 + bodyLength];
      var writer = new ByteWriter(total);

      PacketEncoder.WriteFixedHeader(ref writer, MqttPacketType.Connect, 0, bodyLength);
      writer.WriteBigEndian((ushort)4);
      writer.WriteBytes("MQTT"u8);
      writer.WriteByte(protocolLevel);
      writer.WriteByte(0x02); // CleanStart
      writer.WriteBigEndian((ushort)60);
      writer.WriteByte(0x00); // V5 Connect Properties Length = 0
      writer.WriteBigEndian((ushort)clientIdBytes.Length);
      writer.WriteBytes(clientIdBytes);

      return total[..writer.Position];
   }
}
