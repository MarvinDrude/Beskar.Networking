using System.Buffers.Binary;
using System.Text;
using Beskar.Networking.Raft.Protocol.Models;

namespace Beskar.Networking.Raft.Storage;

/// <summary>
/// Persistent, crash-safe, append-only file-based log and metadata storage for <see cref="RaftNode"/>.
/// </summary>
public sealed class FileRaftLogStorage : IRaftLogStorage
{
   private readonly string _metadataPath;
   private readonly string _logPath;

   private readonly Lock _lock = new();

   private readonly List<RaftLogEntry> _entries = [];
   private readonly List<long> _entryOffsets = [];
   private FileStream? _logFileStream;

   private ulong _currentTerm;
   private string? _votedFor;
   private ulong _compactedUntilIndex;
   private ulong _compactedUntilTerm;
   private int _disposed;

   public FileRaftLogStorage(string storageDirectory)
   {
      Directory.CreateDirectory(storageDirectory);
      _metadataPath = Path.Combine(storageDirectory, "metadata.bin");
      _logPath = Path.Combine(storageDirectory, "raft.log");

      LoadMetadata();
      OpenAndIndexLog();
   }

   private FileStream ThrowIfDisposed()
   {
      if (Volatile.Read(ref _disposed) != 0 || _logFileStream == null)
      {
         throw new ObjectDisposedException(nameof(FileRaftLogStorage));
      }

      return _logFileStream;
   }

   public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         return ValueTask.FromResult(_currentTerm);
      }
   }

   public ValueTask SetCurrentTermAsync(ulong term, CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         _currentTerm = term;
         WriteMetadata();
         return ValueTask.CompletedTask;
      }
   }

   public ValueTask<string?> GetVotedForAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         return ValueTask.FromResult(_votedFor);
      }
   }

   public ValueTask SetVotedForAsync(string? candidateId, CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         _votedFor = candidateId;
         WriteMetadata();
         return ValueTask.CompletedTask;
      }
   }

   public ValueTask SetTermAndVoteAsync(ulong term, string? candidateId, CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         _currentTerm = term;
         _votedFor = candidateId;
         WriteMetadata();
         return ValueTask.CompletedTask;
      }
   }

   public ValueTask<ulong> GetLastLogIndexAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         if (_entries.Count == 0)
         {
            return ValueTask.FromResult(_compactedUntilIndex);
         }

         return ValueTask.FromResult(_entries[^1].Index);
      }
   }

   public ValueTask<ulong> GetLastLogTermAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         if (_entries.Count == 0)
         {
            return ValueTask.FromResult(_compactedUntilTerm);
         }

         return ValueTask.FromResult(_entries[^1].Term);
      }
   }

   public ValueTask<RaftLogEntry?> GetEntryAsync(ulong index, CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         return RaftLogStorageHelper.TryGetEntryOffset(index, _entries, out var listIndex)
            ? ValueTask.FromResult<RaftLogEntry?>(_entries[listIndex])
            : ValueTask.FromResult<RaftLogEntry?>(null);
      }
   }

   public ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount, CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         return ValueTask.FromResult(RaftLogStorageHelper.GetEntriesSlice(_entries, fromIndex, maxCount));
      }
   }

   public ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default)
   {
      lock (_lock)
      {
         var logStream = ThrowIfDisposed();

         logStream.Seek(0, SeekOrigin.End);

         Span<byte> header = stackalloc byte[20];
         for (var i = 0; i < entries.Count; i++)
         {
            var entry = entries[i];
            switch (RaftLogStorageHelper.ClassifyAppendEntry(entry, _entries, _compactedUntilIndex))
            {
               case RaftLogStorageHelper.AppendEntryAction.Skip:
                  continue;

               case RaftLogStorageHelper.AppendEntryAction.TruncateAndAppend:
                  // term conflict: Truncate from this index before overwriting
                  TruncateInternal(entry.Index);
                  logStream.Seek(0, SeekOrigin.End);
                  break;
            }

            var offset = logStream.Position;
            BinaryPrimitives.WriteUInt64LittleEndian(header[..8], entry.Term);
            BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(8, 8), entry.Index);
            BinaryPrimitives.WriteInt32LittleEndian(header.Slice(16, 4), entry.Data.Length);

            logStream.Write(header);
            if (entry.Data.Length > 0)
            {
               logStream.Write(entry.Data.Span);
            }

            _entries.Add(entry);
            _entryOffsets.Add(offset);
         }

         logStream.Flush(flushToDisk: true);
         return ValueTask.CompletedTask;
      }
   }

   public ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         TruncateInternal(fromIndex);
         return ValueTask.CompletedTask;
      }
   }

   private void TruncateInternal(ulong fromIndex)
   {
      if (_logFileStream == null ||
          !RaftLogStorageHelper.TryGetTruncateAction(fromIndex, _entries, out var clearAll, out var startListIndex))
      {
         return;
      }

      if (clearAll)
      {
         _entries.Clear();
         _entryOffsets.Clear();

         _logFileStream.SetLength(0);
         _logFileStream.Flush(flushToDisk: true);
         return;
      }

      var cutOffset = _entryOffsets[startListIndex];

      _entries.RemoveRange(startListIndex, _entries.Count - startListIndex);
      _entryOffsets.RemoveRange(startListIndex, _entryOffsets.Count - startListIndex);

      _logFileStream.SetLength(cutOffset);
      _logFileStream.Flush(flushToDisk: true);
   }

   public ValueTask CompactPrefixAsync(ulong untilIndex, ulong untilTerm = 0, CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         CompactPrefixInternal(untilIndex, untilTerm);
         return ValueTask.CompletedTask;
      }
   }

   private void CompactPrefixInternal(ulong untilIndex, ulong untilTerm = 0)
   {
      if (_logFileStream == null)
      {
         return;
      }

      var plan = RaftLogStorageHelper.PlanCompaction(
         untilIndex,
         untilTerm,
         _entries,
         _compactedUntilIndex,
         _compactedUntilTerm);

      if (!plan.ShouldUpdate)
      {
         return;
      }

      _compactedUntilIndex = plan.NewCompactedUntilIndex;
      _compactedUntilTerm = plan.NewCompactedUntilTerm;

      // Persist compacted metadata before rewriting or truncating the log so a crash cannot leave old entries active.
      WriteMetadata();

      if (plan.ClearAllRemaining)
      {
         _entries.Clear();
         _entryOffsets.Clear();
         _logFileStream.SetLength(0);
         _logFileStream.Flush(flushToDisk: true);
      }
      else if (plan.RemoveCount > 0)
      {
         _entries.RemoveRange(0, plan.RemoveCount);
         _entryOffsets.RemoveRange(0, plan.RemoveCount);
         RewriteLogFileInternal();
      }
   }

   private void RewriteLogFileInternal()
   {
      if (_logFileStream == null) return;

      _logFileStream.SetLength(0);
      _logFileStream.Seek(0, SeekOrigin.Begin);
      _entryOffsets.Clear();

      Span<byte> header = stackalloc byte[20];
      for (var i = 0; i < _entries.Count; i++)
      {
         var entry = _entries[i];
         var offset = _logFileStream.Position;

         BinaryPrimitives.WriteUInt64LittleEndian(header[..8], entry.Term);
         BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(8, 8), entry.Index);
         BinaryPrimitives.WriteInt32LittleEndian(header.Slice(16, 4), entry.Data.Length);

         _logFileStream.Write(header);
         if (entry.Data.Length > 0)
         {
            _logFileStream.Write(entry.Data.Span);
         }

         _entryOffsets.Add(offset);
      }

      _logFileStream.Flush(flushToDisk: true);
   }

   private void LoadMetadata()
   {
      if (!File.Exists(_metadataPath))
      {
         _currentTerm = 0;
         _votedFor = null;
         _compactedUntilIndex = 0;
         _compactedUntilTerm = 0;
         return;
      }

      var bytes = File.ReadAllBytes(_metadataPath);
      if (bytes.Length < 10)
      {
         return;
      }

      _currentTerm = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(0, 8));

      if (bytes.Length >= 26)
      {
         _compactedUntilIndex = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(8, 8));
         _compactedUntilTerm = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(16, 8));
         var votedForLen = BinaryPrimitives.ReadUInt16LittleEndian(bytes.AsSpan(24, 2));

         if (bytes.Length == 26 && votedForLen == ushort.MaxValue)
         {
            _votedFor = null;
         }
         else if (votedForLen > 0 && bytes.Length >= 26 + votedForLen)
         {
            _votedFor = Encoding.UTF8.GetString(bytes, 26, votedForLen);
         }
         else if (votedForLen == 0 && bytes.Length >= 30)
         {
            var extendedLen = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(26, 4));
            if (extendedLen > 0 && bytes.Length >= 30 + extendedLen)
            {
               _votedFor = Encoding.UTF8.GetString(bytes, 30, extendedLen);
            }
            else
            {
               _votedFor = null;
            }
         }
         else
         {
            _votedFor = null;
         }
      }
      else
      {
         _compactedUntilIndex = 0;
         _compactedUntilTerm = 0;
         var votedForLen = BinaryPrimitives.ReadInt16LittleEndian(bytes.AsSpan(8, 2));

         if (votedForLen > 0 && bytes.Length >= 10 + votedForLen)
         {
            _votedFor = Encoding.UTF8.GetString(bytes, 10, votedForLen);
         }
         else
         {
            _votedFor = null;
         }
      }
   }

   private void WriteMetadata()
   {
      Span<byte> header = stackalloc byte[26];
      BinaryPrimitives.WriteUInt64LittleEndian(header[..8], _currentTerm);
      BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(8, 8), _compactedUntilIndex);
      BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(16, 8), _compactedUntilTerm);

      byte[]? votedForBytes = null;
      if (!string.IsNullOrEmpty(_votedFor))
      {
         votedForBytes = Encoding.UTF8.GetBytes(_votedFor);
      }

      byte[] buffer;
      if (votedForBytes == null)
      {
         BinaryPrimitives.WriteInt16LittleEndian(header.Slice(24, 2), -1);
         buffer = new byte[26];
         header.CopyTo(buffer);
      }
      else if (votedForBytes.Length <= ushort.MaxValue)
      {
         BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(24, 2), (ushort)votedForBytes.Length);
         buffer = new byte[26 + votedForBytes.Length];
         header.CopyTo(buffer);
         votedForBytes.CopyTo(buffer, 26);
      }
      else
      {
         BinaryPrimitives.WriteUInt16LittleEndian(header.Slice(24, 2), 0);
         buffer = new byte[30 + votedForBytes.Length];
         header.CopyTo(buffer);
         BinaryPrimitives.WriteInt32LittleEndian(buffer.AsSpan(26, 4), votedForBytes.Length);
         votedForBytes.CopyTo(buffer, 30);
      }

      var tempPath = $"{_metadataPath}.tmp";
      using (var fs = new FileStream(tempPath, FileMode.Create, FileAccess.Write, FileShare.None))
      {
         fs.Write(buffer);
         fs.Flush(flushToDisk: true);
      }

      File.Move(tempPath, _metadataPath, overwrite: true);
   }

   private void OpenAndIndexLog()
   {
      _logFileStream = new FileStream(_logPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);

      _entries.Clear();
      _entryOffsets.Clear();

      var needsTruncateFlush = false;
      var hasSkippedCompacted = false;

      Span<byte> header = stackalloc byte[20];
      while (_logFileStream.Position + 20 <= _logFileStream.Length)
      {
         var offset = _logFileStream.Position;
         var readHeaderBytes = _logFileStream.Read(header);
         if (readHeaderBytes < 20)
         {
            _logFileStream.SetLength(offset);
            needsTruncateFlush = true;
            break;
         }

         var term = BinaryPrimitives.ReadUInt64LittleEndian(header[..8]);
         var index = BinaryPrimitives.ReadUInt64LittleEndian(header.Slice(8, 8));
         var dataLen = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(16, 4));

         if (dataLen < 0 || _logFileStream.Position + dataLen > _logFileStream.Length)
         {
            // Corrupt trailing write, truncate to last valid offset
            _logFileStream.SetLength(offset);
            needsTruncateFlush = true;
            break;
         }

         var data = new byte[dataLen];
         if (dataLen > 0)
         {
            _logFileStream.ReadExactly(data);
         }

         if (index <= _compactedUntilIndex)
         {
            hasSkippedCompacted = true;
            continue;
         }

         _entries.Add(new RaftLogEntry(term, index, data));
         _entryOffsets.Add(offset);
      }

      if (_logFileStream.Position < _logFileStream.Length)
      {
         _logFileStream.SetLength(_logFileStream.Position);
         needsTruncateFlush = true;
      }

      if (hasSkippedCompacted)
      {
         RewriteLogFileInternal();
      }
      else if (needsTruncateFlush)
      {
         _logFileStream.Flush(flushToDisk: true);
      }
   }

   public ValueTask<ulong> GetCompactedUntilIndexAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         return ValueTask.FromResult(_compactedUntilIndex);
      }
   }

   public ValueTask<ulong> GetCompactedUntilTermAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         ThrowIfDisposed();
         return ValueTask.FromResult(_compactedUntilTerm);
      }
   }

   public ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) != 0)
      {
         return ValueTask.CompletedTask;
      }

      lock (_lock)
      {
         if (_logFileStream != null)
         {
            _logFileStream.Flush(flushToDisk: true);
            _logFileStream.Dispose();
            _logFileStream = null;
         }
         _entries.Clear();
         _entryOffsets.Clear();
      }

      return ValueTask.CompletedTask;
   }
}
