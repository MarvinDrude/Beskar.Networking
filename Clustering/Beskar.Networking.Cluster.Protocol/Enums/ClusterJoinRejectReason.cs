using Beskar.Memory.Code.EnumGenerator.Attributes;

namespace Beskar.Networking.Cluster.Protocol.Enums;

[FastEnum]
public enum ClusterJoinRejectReason : byte
{
   None = 0,
   AuthFailed,
   ClusterNameMismatch,
   DuplicateNodeId,
}
