namespace Beskar.Networking.Protocol.Attributes;

public abstract class BaseProtocolAttribute : Attribute
{
   public required int Order { get; set; }
}
