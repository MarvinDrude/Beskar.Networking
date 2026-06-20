using System.Buffers;
using System.Threading.Tasks;
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
}
