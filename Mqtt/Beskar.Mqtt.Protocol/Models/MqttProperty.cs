using System.Buffers;
using System.Runtime.InteropServices;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Extensions;

namespace Beskar.Mqtt.Protocol.Models;

[StructLayout(LayoutKind.Auto)]
public readonly ref struct MqttProperty(
   PropertyIdentifier identifier,
   ReadOnlySequence<byte> valueBytes)
{
   public PropertyIdentifier Identifier { get; } = identifier;
   public ReadOnlySequence<byte> ValueBytes { get; } = valueBytes;

   public byte AsByte() => ValueBytes.FirstSpan[0];

   public ushort AsTwoByteInteger()
   {
      var reader = new SequenceReader<byte>(ValueBytes);
      reader.TryReadBigEndian(out short val);

      return (ushort)val;
   }

   public uint AsFourByteInteger()
   {
      var reader = new SequenceReader<byte>(ValueBytes);
      reader.TryReadBigEndian(out int val);

      return (uint)val;
   }

   public UserPropertyPair AsUserProperty()
   {
      var reader = new SequenceReader<byte>(ValueBytes);
      reader.TryReadRawBytes(out var keyBytes);
      reader.TryReadRawBytes(out var valueBytes);

      return new UserPropertyPair(keyBytes, valueBytes);
   }
}

[StructLayout(LayoutKind.Auto)]
public readonly ref struct UserPropertyPair(
   ReadOnlySequence<byte> keyBytes,
   ReadOnlySequence<byte> valueBytes)
{
   public ReadOnlySequence<byte> KeyBytes { get; } = keyBytes;
   public ReadOnlySequence<byte> ValueBytes { get; } = valueBytes;
}
