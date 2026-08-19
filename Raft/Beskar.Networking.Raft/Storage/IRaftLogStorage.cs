using Beskar.Networking.Raft.Protocol.Models;

namespace Beskar.Networking.Raft.Storage;

/// <summary>
/// Defines the storage contract for Raft persistent state (current term, voted-for candidate, and log entries).
/// </summary>
public interface IRaftLogStorage : IAsyncDisposable
{
   /// <summary>
   /// Gets the latest term server has seen. Initialized to 0 on first boot.
   /// </summary>
   public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default);

   /// <summary>
   /// Updates the current term.
   /// </summary>
   public ValueTask SetCurrentTermAsync(ulong term, CancellationToken ct = default);

   /// <summary>
   /// Gets candidateId that received vote in current term (or null if none).
   /// </summary>
   public ValueTask<string?> GetVotedForAsync(CancellationToken ct = default);

   /// <summary>
   /// Updates candidateId that received vote in current term.
   /// </summary>
   public ValueTask SetVotedForAsync(string? candidateId, CancellationToken ct = default);

   /// <summary>
   /// Updates both term and votedFor atomically.
   /// </summary>
   public ValueTask SetTermAndVoteAsync(ulong term, string? candidateId, CancellationToken ct = default);

   /// <summary>
   /// Gets the highest log entry index in the log. Returns 0 if log is empty.
   /// </summary>
   public ValueTask<ulong> GetLastLogIndexAsync(CancellationToken ct = default);

   /// <summary>
   /// Gets the term of the highest log entry in the log. Returns 0 if log is empty.
   /// </summary>
   public ValueTask<ulong> GetLastLogTermAsync(CancellationToken ct = default);

   /// <summary>
   /// Retrieves a log entry at the specified index. Returns null if not present.
   /// </summary>
   public ValueTask<RaftLogEntry?> GetEntryAsync(ulong index, CancellationToken ct = default);

   /// <summary>
   /// Retrieves a batch of entries starting from <paramref name="fromIndex"/> up to <paramref name="maxCount"/>.
   /// </summary>
   public ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount, CancellationToken ct = default);

   /// <summary>
   /// Appends a sequence of new log entries to the log.
   /// </summary>
   public ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default);

   /// <summary>
   /// Truncates all log entries starting from <paramref name="fromIndex"/> onwards (inclusive).
   /// </summary>
   public ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default);

   /// <summary>
   /// Compacts and discards all historical log entries up to and including <paramref name="untilIndex"/>.
   /// </summary>
   public ValueTask CompactPrefixAsync(ulong untilIndex, CancellationToken ct = default);
}
