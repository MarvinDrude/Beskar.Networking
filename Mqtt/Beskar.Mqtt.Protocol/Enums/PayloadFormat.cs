using Beskar.Memory.Code.EnumGenerator.Attributes;

namespace Beskar.Mqtt.Protocol.Enums;

[FastEnum]
public enum PayloadFormat : byte
{
   Unspecified = 0,
   CharacterData = 1
}
