using Beskar.Memory.Code.PacketGenerator.Attributes;
using Beskar.Networking.Common.Registries;

namespace Beskar.Networking.Cluster.Protocol.Registries;

[PacketRegistry<object>]
public sealed partial class ClusterMessageRegistry
   : BeskarRegistry<object>;
