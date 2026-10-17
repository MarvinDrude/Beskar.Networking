using Beskar.Jobs.Common.Attributes;
using Beskar.Jobs.Common.Interfaces;
using Beskar.Jobs.Example.Distributed.Common;

namespace Beskar.Jobs.Example.Distributed.Worker;

[GeneratedJobHandler(DistributedClusterConfig.ClusterHealthAuditJobType, Queue = DistributedClusterConfig.OpsQueue)]
public sealed partial class ClusterHealthAuditJobHandler : IJobHandler<ClusterHealthAuditArgs>
{
   public async ValueTask ExecuteAsync(
      IJobExecutionContext<ClusterHealthAuditArgs> context,
      CancellationToken ct = default)
   {
      await context.ReportProgressAsync(50, $"[{context.WorkerId}] Auditing region '{context.Arguments.Region}'...", ct);
      await Task.Delay(150, ct);
      Console.WriteLine($"[Worker {DateTime.Now:HH:mm:ss}] Health Audit OK for region '{context.Arguments.Region}' (StorageCheck={context.Arguments.IncludeStorageCheck})");
   }
}
