using System.Buffers;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
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

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public ushort AsTwoByteInteger()
   {
      var reader = new SequenceReader<byte>(ValueBytes);
      reader.TryReadBigEndian(out short val);

      return (ushort)val;
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public uint AsFourByteInteger()
   {
      var reader = new SequenceReader<byte>(ValueBytes);
      reader.TryReadBigEndian(out int val);

      return (uint)val;
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public ReadOnlySequence<byte> AsRawBytes()
   {
      var reader = new SequenceReader<byte>(ValueBytes);
      reader.TryReadRawBytes(out var value);

      return value;
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public ReadOnlySequence<byte> AsRawString()
   {
      var reader = new SequenceReader<byte>(ValueBytes);
      reader.TryReadRawString(out var value);

      return value;
   }

   [MethodImpl(MethodImplOptions.AggressiveInlining)]
   public UserPropertyPair AsUserProperty()
   {
      var reader = new SequenceReader<byte>(ValueBytes);
      reader.TryReadRawBytes(out var keyBytes);
      reader.TryReadRawBytes(out var valueBytes);

      return new UserPropertyPair(keyBytes, valueBytes);
   }

   public uint AsSessionExpiryInterval() => AsFourByteInteger();
   public ReadOnlySequence<byte> AsAuthenticationMethod() => AsRawString();
   public ReadOnlySequence<byte> AsAuthenticationData() => AsRawBytes();
   public ReadOnlySequence<byte> AsAssignedClientIdentifier() => AsRawString();
   public ReadOnlySequence<byte> AsContentType() => AsRawString();
   public ReadOnlySequence<byte> AsCorrelationData() => AsRawBytes();
   public uint AsMessageExpiryInterval() => AsFourByteInteger();
   public uint AsMaximumPacketSize() => AsFourByteInteger();
   public PayloadFormat AsPayloadFormat() => (PayloadFormat)AsByte();
   public bool AsRequestResponseInfo() => AsByte() == 1;
   public uint AsWillDelayInterval() => AsFourByteInteger();
   public bool AsRequestProblemInfo() => AsByte() == 1;
   public bool AsWildcardSubscriptionAvailable() => AsByte() == 1;
   public ushort AsTopicAlias() => AsTwoByteInteger();
   public ushort AsTopicAliasMaximum() => AsTwoByteInteger();
   public bool AsSubscriptionIdentifierAvailable() => AsByte() == 1;
   public uint AsSubscriptionIdentifier() => AsFourByteInteger();
   public bool AsSharedSubscriptionAvailable() => AsByte() == 1;
   public ReadOnlySequence<byte> AsServerReference() => AsRawString();
   public bool AsRetainAvailable() => AsByte() == 1;
   public ushort AsServerKeepAlive() => AsTwoByteInteger();
   public ReadOnlySequence<byte> AsResponseTopic() => AsRawString();
   public ReadOnlySequence<byte> AsResponseInfo() => AsRawString();
   public ushort AsReceiveMaximum() => AsTwoByteInteger();
   public ReadOnlySequence<byte> AsReasonString() => AsRawString();

   public Result<QualityOfServiceType, StringError> AsMaximumQualityOfService()
   {
      if (AsByte() is < 1 and var raw)
      {
         return (QualityOfServiceType)raw;
      }

      return new StringError("Invalid maximum QualityOfService.");
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
