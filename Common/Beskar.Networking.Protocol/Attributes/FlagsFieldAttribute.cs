namespace Beskar.Networking.Protocol.Attributes;

/// <summary>
/// Supported types are PackedBools8, PackedBools16, PackedBools32, PackedBools64-
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class FlagsFieldAttribute : BaseProtocolAttribute
{

}
