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
   private int _disposed;

   public FileRaftLogStorage(string storageDirectory)
   {
      Directory.CreateDirectory(storageDirectory);
      _metadataPath = Path.Combine(storageDirectory, "metadata.bin");
      _logPath = Path.Combine(storageDirectory, "raft.log");

      LoadMetadata();
      OpenAndIndexLog();
   }

   public ValueTask<ulong> GetCurrentTermAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         return ValueTask.FromResult(_currentTerm);
      }
   }

   public ValueTask SetCurrentTermAsync(ulong term, CancellationToken ct = default)
   {
      lock (_lock)
      {
         _currentTerm = term;
         WriteMetadata();
         return ValueTask.CompletedTask;
      }
   }

   public ValueTask<string?> GetVotedForAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         return ValueTask.FromResult(_votedFor);
      }
   }

   public ValueTask SetVotedForAsync(string? candidateId, CancellationToken ct = default)
   {
      lock (_lock)
      {
         _votedFor = candidateId;
         WriteMetadata();
         return ValueTask.CompletedTask;
      }
   }

   public ValueTask SetTermAndVoteAsync(ulong term, string? candidateId, CancellationToken ct = default)
   {
      lock (_lock)
      {
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
         if (_entries.Count == 0)
         {
            return ValueTask.FromResult(0UL);
         }

         return ValueTask.FromResult(_entries[^1].Index);
      }
   }

   public ValueTask<ulong> GetLastLogTermAsync(CancellationToken ct = default)
   {
      lock (_lock)
      {
         if (_entries.Count == 0)
         {
            return ValueTask.FromResult(0UL);
         }

         return ValueTask.FromResult(_entries[^1].Term);
      }
   }

   public ValueTask<RaftLogEntry?> GetEntryAsync(ulong index, CancellationToken ct = default)
   {
      lock (_lock)
      {
         if (index == 0 || _entries.Count == 0)
         {
            return ValueTask.FromResult<RaftLogEntry?>(null);
         }

         var firstIndex = _entries[0].Index;
         if (index < firstIndex)
         {
            return ValueTask.FromResult<RaftLogEntry?>(null);
         }

         var listIndex = (int)(index - firstIndex);
         if (listIndex < _entries.Count)
         {
            return ValueTask.FromResult<RaftLogEntry?>(_entries[listIndex]);
         }

         return ValueTask.FromResult<RaftLogEntry?>(null);
      }
   }

   public ValueTask<IReadOnlyList<RaftLogEntry>> GetEntriesAsync(ulong fromIndex, int maxCount, CancellationToken ct = default)
   {
      lock (_lock)
      {
         if (fromIndex == 0 || _entries.Count == 0 || maxCount <= 0)
         {
            return ValueTask.FromResult<IReadOnlyList<RaftLogEntry>>(Array.Empty<RaftLogEntry>());
         }

         var firstIndex = _entries[0].Index;
         if (fromIndex < firstIndex)
         {
            return ValueTask.FromResult<IReadOnlyList<RaftLogEntry>>(Array.Empty<RaftLogEntry>());
         }

         var startListIndex = (int)(fromIndex - firstIndex);
         if (startListIndex >= _entries.Count)
         {
            return ValueTask.FromResult<IReadOnlyList<RaftLogEntry>>(Array.Empty<RaftLogEntry>());
         }

         var count = Math.Min(maxCount, _entries.Count - startListIndex);
         var result = new List<RaftLogEntry>(count);
         for (var i = 0; i < count; i++)
         {
            result.Add(_entries[startListIndex + i]);
         }

         return ValueTask.FromResult<IReadOnlyList<RaftLogEntry>>(result);
      }
   }

   public ValueTask AppendEntriesAsync(IReadOnlyList<RaftLogEntry> entries, CancellationToken ct = default)
   {
      lock (_lock)
      {
         if (_logFileStream == null)
         {
            throw new ObjectDisposedException(nameof(FileRaftLogStorage));
         }

         _logFileStream.Seek(0, SeekOrigin.End);

         Span<byte> header = stackalloc byte[20];
         for (var i = 0; i < entries.Count; i++)
         {
            var entry = entries[i];
            if (_entries.Count > 0)
            {
               var firstIndex = _entries[0].Index;
               var existingListIndex = (int)(entry.Index - firstIndex);

               if (existingListIndex >= 0 && existingListIndex < _entries.Count)
               {
                  // Truncate from this index before overwriting
                  TruncateInternal(entry.Index);
               }
            }

            var offset = _logFileStream.Position;
            BinaryPrimitives.WriteUInt64LittleEndian(header[..8], entry.Term);
            BinaryPrimitives.WriteUInt64LittleEndian(header.Slice(8, 8), entry.Index);
            BinaryPrimitives.WriteInt32LittleEndian(header.Slice(16, 4), entry.Data.Length);

            _logFileStream.Write(header);
            if (entry.Data.Length > 0)
            {
               _logFileStream.Write(entry.Data.Span);
            }

            _entries.Add(entry);
            _entryOffsets.Add(offset);
         }

         _logFileStream.Flush(flushToDisk: true);
         return ValueTask.CompletedTask;
      }
   }

   public ValueTask TruncateLogAsync(ulong fromIndex, CancellationToken ct = default)
   {
      lock (_lock)
      {
         TruncateInternal(fromIndex);
         return ValueTask.CompletedTask;
      }
   }

   private void TruncateInternal(ulong fromIndex)
   {
      if (_entries.Count == 0 || fromIndex == 0 || _logFileStream == null)
      {
         return;
      }

      var firstIndex = _entries[0].Index;
      if (fromIndex < firstIndex)
      {
         _entries.Clear();
         _entryOffsets.Clear();

         _logFileStream.SetLength(0);
         _logFileStream.Flush(flushToDisk: true);
         return;
      }

      var startListIndex = (int)(fromIndex - firstIndex);
      if (startListIndex < _entries.Count)
      {
         var cutOffset = _entryOffsets[startListIndex];

         _entries.RemoveRange(startListIndex, _entries.Count - startListIndex);
         _entryOffsets.RemoveRange(startListIndex, _entryOffsets.Count - startListIndex);

         _logFileStream.SetLength(cutOffset);
         _logFileStream.Flush(flushToDisk: true);
      }
   }

   private void LoadMetadata()
   {
      if (!File.Exists(_metadataPath))
      {
         _currentTerm = 0;
         _votedFor = null;
         return;
      }

      var bytes = File.ReadAllBytes(_metadataPath);
      if (bytes.Length < 10)
      {
         return;
      }

      _currentTerm = BinaryPrimitives.ReadUInt64LittleEndian(bytes.AsSpan(0, 8));
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

   private void WriteMetadata()
   {
      Span<byte> header = stackalloc byte[10];
      BinaryPrimitives.WriteUInt64LittleEndian(header[..8], _currentTerm);

      byte[]? votedForBytes = null;
      if (!string.IsNullOrEmpty(_votedFor))
      {
         votedForBytes = Encoding.UTF8.GetBytes(_votedFor);
         BinaryPrimitives.WriteInt16LittleEndian(header.Slice(8, 2), (short)votedForBytes.Length);
      }
      else
      {
         BinaryPrimitives.WriteInt16LittleEndian(header.Slice(8, 2), -1);
      }

      var totalLen = 10 + (votedForBytes?.Length ?? 0);
      var buffer = new byte[totalLen];
      header.CopyTo(buffer);
      if (votedForBytes != null)
      {
         votedForBytes.CopyTo(buffer, 10);
      }

      File.WriteAllBytes(_metadataPath, buffer);
   }

   private void OpenAndIndexLog()
   {
      _logFileStream = new FileStream(_logPath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.Read);

      _entries.Clear();
      _entryOffsets.Clear();

      Span<byte> header = stackalloc byte[20];
      while (_logFileStream.Position + 20 <= _logFileStream.Length)
      {
         var offset = _logFileStream.Position;
         var readHeaderBytes = _logFileStream.Read(header);
         if (readHeaderBytes < 20)
         {
            break;
         }

         var term = BinaryPrimitives.ReadUInt64LittleEndian(header[..8]);
         var index = BinaryPrimitives.ReadUInt64LittleEndian(header.Slice(8, 8));
         var dataLen = BinaryPrimitives.ReadInt32LittleEndian(header.Slice(16, 4));

         if (dataLen < 0 || _logFileStream.Position + dataLen > _logFileStream.Length)
         {
            // Corrupt trailing write, truncate to last valid offset
            _logFileStream.SetLength(offset);
            break;
         }

         var data = new byte[dataLen];
         if (dataLen > 0)
         {
            _logFileStream.ReadExactly(data);
         }

         _entries.Add(new RaftLogEntry(term, index, data));
         _entryOffsets.Add(offset);
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
