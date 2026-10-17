using System.Net;

namespace Beskar.Jobs.Example.Distributed.Common;

public static class DistributedClusterConfig
{
   public const int BrokerPort = 18890;
   public static readonly IPEndPoint BrokerEndPoint = new(IPAddress.Loopback, BrokerPort);

   public const string MediaQueue = "media";
   public const string OpsQueue = "ops";

   public const string VideoTranscodeJobType = "media.video.transcode";
   public const string ThumbnailGenerateJobType = "media.video.thumbnail";
   public const string ClusterHealthAuditJobType = "ops.cluster.audit";
}
