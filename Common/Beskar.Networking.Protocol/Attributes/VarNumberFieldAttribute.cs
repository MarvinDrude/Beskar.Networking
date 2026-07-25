namespace Beskar.Networking.Protocol.Attributes;

/// <summary>
/// Specifies that the field is a variable-length number.
/// Smaller numbers are encoded with fewer bytes.
/// </summary>
[AttributeUsage(AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public class VarNumberFieldAttribute : BaseProtocolAttribute
{

}
