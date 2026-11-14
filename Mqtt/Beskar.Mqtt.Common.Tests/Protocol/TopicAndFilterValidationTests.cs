using System.Buffers;
using System.Text;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Validators.Common;
using Beskar.Mqtt.Protocol.Validators.Version5;

namespace Beskar.Mqtt.Common.Tests.Protocol;

public class TopicAndFilterValidationTests
{
   [Test]
   public async Task SubscribePacketValidator_SecondFilterWithReservedBitsSet_ShouldFail()
   {
      var filters = new byte[64];
      var writer = new ByteWriter(filters);

      WriteFilter(ref writer, "valid/topic", 0x01);
      WriteFilter(ref writer, "second/topic", 0x80); // Reserved bits 6..7 non-zero

      var packet = new SubscribePacket
      {
         PacketIdentifier = 1,
         FiltersBytes = new ReadOnlySequence<byte>(filters[..writer.Position]),
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      var result = SubscribePacketVersion5Validator.Validate(ref packet);

      await Assert.That(result.Failed).IsTrue();
   }

   [Test]
   public async Task UnsubscribePacketValidator_EmptyFilters_ShouldFail()
   {
      var packet = new UnsubscribePacket
      {
         PacketIdentifier = 1,
         FiltersBytes = ReadOnlySequence<byte>.Empty,
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      var result = UnsubscribePacketVersion5Validator.Validate(ref packet);

      await Assert.That(result.Failed).IsTrue();
   }

   [Test]
   public async Task TopicStringValidator_MalformedWildcardPlacement_ShouldFail()
   {
      await Assert.That(TopicStringValidator.ValidateForSubscribe("a#"u8).Failed).IsTrue();
      await Assert.That(TopicStringValidator.ValidateForSubscribe("a#b"u8).Failed).IsTrue();
      await Assert.That(TopicStringValidator.ValidateForSubscribe("sport/tennis#"u8).Failed).IsTrue();
      await Assert.That(TopicStringValidator.ValidateForSubscribe("a+b"u8).Failed).IsTrue();
      await Assert.That(TopicStringValidator.ValidateForSubscribe("+a"u8).Failed).IsTrue();
      await Assert.That(TopicStringValidator.ValidateForSubscribe("sport/a+b/player"u8).Failed).IsTrue();
   }

   [Test]
   public async Task TopicStringValidator_NullCharacterAndInvalidUtf8_ShouldFail()
   {
      byte[] invalidUtf8 = [(byte)'a', 0xFF, 0xFE, (byte)'b'];
      var withNull = "topic\0name"u8.ToArray();

      await Assert.That(TopicStringValidator.Validate(withNull).Failed).IsTrue();
      await Assert.That(TopicStringValidator.ValidateForSubscribe(withNull).Failed).IsTrue();

      await Assert.That(TopicStringValidator.Validate(invalidUtf8).Failed).IsTrue();
      await Assert.That(TopicStringValidator.ValidateForSubscribe(invalidUtf8).Failed).IsTrue();
   }

   private static void WriteFilter(ref ByteWriter writer, string filter, byte optionsByte)
   {
      var bytes = Encoding.UTF8.GetBytes(filter);
      writer.WriteBigEndian((ushort)bytes.Length);
      writer.WriteBytes(bytes);
      writer.WriteByte(optionsByte);
   }
}
