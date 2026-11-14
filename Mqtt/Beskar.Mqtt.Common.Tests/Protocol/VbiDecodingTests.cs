using System.Buffers;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Parsing.Results;

namespace Beskar.Mqtt.Common.Tests.Protocol;

public class VbiDecodingTests
{
   [Test]
   public async Task TryReadVariableByteInteger_FifthBytePresent_ShouldReturnExceedMaxValueWithZeroValue()
   {
      byte[] encoded = [0x80, 0x80, 0x80, 0x80, 0x7F];
      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(encoded));

      var result = reader.TryReadVariableByteInteger(out var value);

      await Assert.That(result).IsEqualTo(VariableByteIntegerResult.ExceedMaxValue);
      await Assert.That(value).IsEqualTo(0U);
   }

   [Test]
   public async Task TryReadVariableByteInteger_TruncatedFourBytesWithContinuationBit_ShouldReturnExceedMaxValue()
   {
      byte[] encoded = [0x80, 0x80, 0x80, 0x80];
      var reader = new SequenceReader<byte>(new ReadOnlySequence<byte>(encoded));

      var result = reader.TryReadVariableByteInteger(out var value);

      await Assert.That(result).IsEqualTo(VariableByteIntegerResult.ExceedMaxValue);
      await Assert.That(value).IsEqualTo(0U);
   }
}
