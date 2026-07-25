namespace Beskar.Networking.Protocol.Attributes;

[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class MagicBytesAttribute(params byte[] magicBytes) : BaseProtocolAttribute
{
   public byte[] MagicBytes { get; } = magicBytes;
}
