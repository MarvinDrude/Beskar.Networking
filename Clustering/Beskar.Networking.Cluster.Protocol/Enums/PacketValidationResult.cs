using Beskar.Memory.Code.EnumGenerator.Attributes;

namespace Beskar.Networking.Cluster.Protocol.Enums;

[FastEnum]
public enum PacketValidationResult : byte
{
   Valid = 1,
   WrongMagic,
   WrongVersion,
   OutdatedEpoch,
   NotJoinedYet
}
