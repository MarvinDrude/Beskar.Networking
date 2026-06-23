namespace Beskar.Mqtt.Protocol.Models;

public readonly ref struct UserProperty(ReadOnlySpan<byte> key, ReadOnlySpan<byte> value)
{
   public ReadOnlySpan<byte> KeyUtf8Bytes { get; } = key;

   public ReadOnlySpan<byte> ValueBytes { get; } = value;
}
