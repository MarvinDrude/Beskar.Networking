using Beskar.Memory.Code.PacketGenerator.Attributes;
using Beskar.Networking.Cluster.Protocol.Models;
using Beskar.Networking.Common.Registries;

namespace Beskar.Networking.Cluster.Protocol.Registries;

[PacketRegistry<ClusterMessageContext>]
public sealed partial class ClusterMessageRegistry
   : BeskarRegistry<ClusterMessageContext>;
