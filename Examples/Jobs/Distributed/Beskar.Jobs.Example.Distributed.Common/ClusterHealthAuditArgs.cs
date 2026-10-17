namespace Beskar.Jobs.Example.Distributed.Common;

public sealed record ClusterHealthAuditArgs(
   string Region,
   bool IncludeStorageCheck);
