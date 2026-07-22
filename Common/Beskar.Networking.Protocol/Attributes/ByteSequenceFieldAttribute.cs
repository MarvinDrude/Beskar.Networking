namespace Beskar.Networking.Protocol.Attributes;

/// <summary>
/// A byte sequence field of variable length.
/// </summary>
/// <param name="lengthPropertyName">The name of the property used for the length of the byte sequence.</param>
/// <param name="safeCopyData">
/// If true, the byte sequence will be copied from the rented buffer into new heap allocated memory.
/// (if not needed beyond the lifetime of the event handler, you can optimize to avoid the alloc & copy)
/// </param>
[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class ByteSequenceFieldAttribute(
   string lengthPropertyName,
   bool safeCopyData = true)
   : BaseProtocolAttribute
{
   public string LengthPropertyName { get; } = lengthPropertyName;

   public bool SafeCopyData { get; } = safeCopyData;
}
