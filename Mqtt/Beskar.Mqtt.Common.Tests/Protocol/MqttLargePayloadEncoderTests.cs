using System.Buffers;
using System.IO.Pipelines;
using System.Text;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Encoders.Version3;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Common.Parsers;
using Beskar.Mqtt.Common.Tests.Helpers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Tests.Protocol;

public class MqttLargePayloadEncoderTests
{
   private sealed class TrackingPipeWriter(PipeWriter inner) : PipeWriter
   {
      public List<int> RequestedSpanSizes { get; } = [];

      public override Span<byte> GetSpan(int sizeHint = 0)
      {
         RequestedSpanSizes.Add(sizeHint);
         return inner.GetSpan(sizeHint);
      }

      public override Memory<byte> GetMemory(int sizeHint = 0)
      {
         return inner.GetMemory(sizeHint);
      }

      public override void Advance(int bytes) => inner.Advance(bytes);
      public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default) => inner.FlushAsync(cancellationToken);
      public override void CancelPendingFlush() => inner.CancelPendingFlush();
      public override void Complete(Exception? exception = null) => inner.Complete(exception);
   }

   [Test]
   public async Task Version5_WritePublishPacket_WithPayloadGreaterThan4Kb_UsesGetSpanAndParsesSuccessfully()
   {
      var pipe = new Pipe();
      var trackingWriter = new TrackingPipeWriter(pipe.Writer);

      const int payloadSize = 8192; // 8KB > 4KB
      var originalPayload = new byte[payloadSize];
      Random.Shared.NextBytes(originalPayload);

      var packet = new PublishPacket
      {
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/v5/large"u8.ToArray()),
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         PacketIdentifier = 101,
         Payload = new ReadOnlySequence<byte>(originalPayload),
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      var encoder = new PacketVersion5Encoder(trackingWriter);
      encoder.WritePublish(in packet);

      // Verify that GetSpan was called with sizeHint > 4096 (which includes header + topic + payload)
      await Assert.That(trackingWriter.RequestedSpanSizes.Any(size => size >= payloadSize)).IsTrue();

      await pipe.Writer.FlushAsync();

      var readResult = await pipe.Reader.ReadAsync();
      var buffer = readResult.Buffer;

      var wasInvoked = false;
      byte[]? parsedPayload = null;

      var handler = new TestPacketHandler
      {
         OnPublish = (in p) =>
         {
            wasInvoked = true;
            parsedPayload = new byte[p.Payload.Length];
            p.Payload.CopyTo(parsedPayload);
         }
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var sequenceReader = new SequenceReader<byte>(buffer);
      var dispatchResult = await parser.TryDispatch(ref sequenceReader, out _);

      await Assert.That(dispatchResult.Failed).IsFalse();
      await Assert.That(dispatchResult.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPayload).IsNotNull();
      await Assert.That(parsedPayload!.Length).IsEqualTo(payloadSize);
      await Assert.That(parsedPayload).IsEquivalentTo(originalPayload);

      pipe.Reader.AdvanceTo(buffer.End);
   }

   [Test]
   public async Task Version5_WritePublishOptions_WithPayloadGreaterThan4Kb_UsesGetSpanAndParsesSuccessfully()
   {
      var pipe = new Pipe();
      var trackingWriter = new TrackingPipeWriter(pipe.Writer);

      const int payloadSize = 65536; // 64KB > 4KB
      var originalPayload = new byte[payloadSize];
      Random.Shared.NextBytes(originalPayload);

      var options = new PublishOptionsBuilder()
         .WithTopic("test/v5/large/heap")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload(originalPayload)
         .Build();

      var encoder = new PacketVersion5Encoder(trackingWriter);
      encoder.WritePublish(options, 202);

      // Verify that GetSpan was called with sizeHint >= 65536
      await Assert.That(trackingWriter.RequestedSpanSizes.Any(size => size >= payloadSize)).IsTrue();

      await pipe.Writer.FlushAsync();

      var readResult = await pipe.Reader.ReadAsync();
      var buffer = readResult.Buffer;

      var wasInvoked = false;
      byte[]? parsedPayload = null;

      var handler = new TestPacketHandler
      {
         OnPublish = (in p) =>
         {
            wasInvoked = true;
            parsedPayload = new byte[p.Payload.Length];
            p.Payload.CopyTo(parsedPayload);
         }
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V50);
      var sequenceReader = new SequenceReader<byte>(buffer);
      var dispatchResult = await parser.TryDispatch(ref sequenceReader, out _);

      await Assert.That(dispatchResult.Failed).IsFalse();
      await Assert.That(dispatchResult.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPayload).IsNotNull();
      await Assert.That(parsedPayload!.Length).IsEqualTo(payloadSize);
      await Assert.That(parsedPayload).IsEquivalentTo(originalPayload);

      pipe.Reader.AdvanceTo(buffer.End);
   }

   [Test]
   public async Task Version3_WritePublishPacket_WithPayloadGreaterThan4Kb_UsesGetSpanAndParsesSuccessfully()
   {
      var pipe = new Pipe();
      var trackingWriter = new TrackingPipeWriter(pipe.Writer);

      const int payloadSize = 8192; // 8KB > 4KB
      var originalPayload = new byte[payloadSize];
      Random.Shared.NextBytes(originalPayload);

      var packet = new PublishPacket
      {
         TopicUtf8Bytes = new ReadOnlySequence<byte>("test/v3/large"u8.ToArray()),
         QualityOfService = QualityOfServiceType.AtLeastOnce,
         PacketIdentifier = 303,
         Payload = new ReadOnlySequence<byte>(originalPayload),
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      var encoder = new PacketVersion3Encoder(trackingWriter, MqttProtocolVersion.V311);
      encoder.WritePublish(in packet);

      await Assert.That(trackingWriter.RequestedSpanSizes.Any(size => size >= payloadSize)).IsTrue();

      await pipe.Writer.FlushAsync();

      var readResult = await pipe.Reader.ReadAsync();
      var buffer = readResult.Buffer;

      var wasInvoked = false;
      byte[]? parsedPayload = null;

      var handler = new TestPacketHandler
      {
         OnPublish = (in p) =>
         {
            wasInvoked = true;
            parsedPayload = new byte[p.Payload.Length];
            p.Payload.CopyTo(parsedPayload);
         }
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V311);
      var sequenceReader = new SequenceReader<byte>(buffer);
      var dispatchResult = await parser.TryDispatch(ref sequenceReader, out _);

      await Assert.That(dispatchResult.Failed).IsFalse();
      await Assert.That(dispatchResult.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPayload).IsNotNull();
      await Assert.That(parsedPayload!.Length).IsEqualTo(payloadSize);
      await Assert.That(parsedPayload).IsEquivalentTo(originalPayload);

      pipe.Reader.AdvanceTo(buffer.End);
   }

   [Test]
   public async Task Version3_WritePublishOptions_WithPayloadGreaterThan4Kb_UsesGetSpanAndParsesSuccessfully()
   {
      var pipe = new Pipe();
      var trackingWriter = new TrackingPipeWriter(pipe.Writer);

      const int payloadSize = 65536; // 64KB > 4KB
      var originalPayload = new byte[payloadSize];
      Random.Shared.NextBytes(originalPayload);

      var options = new PublishOptionsBuilder()
         .WithTopic("test/v3/large/heap")
         .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
         .WithPayload(originalPayload)
         .Build();

      var encoder = new PacketVersion3Encoder(trackingWriter, MqttProtocolVersion.V311);
      encoder.WritePublish(options, 404);

      await Assert.That(trackingWriter.RequestedSpanSizes.Any(size => size >= payloadSize)).IsTrue();

      await pipe.Writer.FlushAsync();

      var readResult = await pipe.Reader.ReadAsync();
      var buffer = readResult.Buffer;

      var wasInvoked = false;
      byte[]? parsedPayload = null;

      var handler = new TestPacketHandler
      {
         OnPublish = (in p) =>
         {
            wasInvoked = true;
            parsedPayload = new byte[p.Payload.Length];
            p.Payload.CopyTo(parsedPayload);
         }
      };

      var parser = new PacketParser(new DummyNetworkStream(), handler, MqttProtocolVersion.V311);
      var sequenceReader = new SequenceReader<byte>(buffer);
      var dispatchResult = await parser.TryDispatch(ref sequenceReader, out _);

      await Assert.That(dispatchResult.Failed).IsFalse();
      await Assert.That(dispatchResult.Success).IsEqualTo(PacketDispatchResult.Success);
      await Assert.That(wasInvoked).IsTrue();
      await Assert.That(parsedPayload).IsNotNull();
      await Assert.That(parsedPayload!.Length).IsEqualTo(payloadSize);
      await Assert.That(parsedPayload).IsEquivalentTo(originalPayload);

      pipe.Reader.AdvanceTo(buffer.End);
   }
}
