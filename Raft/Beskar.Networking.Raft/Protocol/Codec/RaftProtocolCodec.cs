using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Beskar.Networking.Raft.Protocol.Enums;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;

namespace Beskar.Networking.Raft.Protocol.Codec;

/// <summary>
/// High-performance, low-allocation binary encoder and decoder for Raft consensus protocol packets.
/// </summary>
public static class RaftProtocolCodec
{
   public const byte MagicByte1 = 0xBE;
   public const byte MagicByte2 = 0x52; // 'R' for Raft
   public const byte CurrentVersion = 0x01;
   public const int HeaderSize = 8; // Magic (2) + Version (1) + MessageType (1) + PayloadLength (4)

   /// <summary>
   /// Attempts to read a full Raft packet from the sequence reader.
   /// Returns true if a full frame was parsed; false if more data is needed or corrupted.
   /// </summary>
   public static bool TryReadFrame(
      ref SequenceReader<byte> reader,
      out RaftMessageType messageType,
      out object? payload)
   {
      messageType = default;
      payload = null;

      while (reader.Remaining >= HeaderSize)
      {
         var headerReader = reader;
         if (!headerReader.TryRead(out var m1) || m1 != MagicByte1 ||
             !headerReader.TryRead(out var m2) || m2 != MagicByte2)
         {
            reader.Advance(1);
            continue;
         }

         if (!headerReader.TryRead(out var version) || version != CurrentVersion)
         {
            reader.Advance(1);
            continue;
         }

         if (!headerReader.TryRead(out var typeByte))
         {
            reader.Advance(1);
            continue;
         }

         if (!headerReader.TryReadLittleEndian(out int payloadLength) || payloadLength < 0)
         {
            reader.Advance(1);
            continue;
         }

         if (headerReader.Remaining < payloadLength)
         {
            // Incomplete payload, wait for more data
            return false;
         }

         // We have the complete frame! Advance the main reader past header.
         reader = headerReader;
         var payloadSequence = reader.Sequence.Slice(reader.Position, payloadLength);
         reader.Advance(payloadLength);

         var payloadReader = new SequenceReader<byte>(payloadSequence);
         messageType = (RaftMessageType)typeByte;

         payload = messageType switch
         {
            RaftMessageType.RequestVote => ReadRequestVote(ref payloadReader),
            RaftMessageType.RequestVoteResponse => ReadRequestVoteResponse(ref payloadReader),
            RaftMessageType.AppendEntries => ReadAppendEntries(ref payloadReader),
            RaftMessageType.AppendEntriesResponse => ReadAppendEntriesResponse(ref payloadReader),
            RaftMessageType.InstallSnapshot => ReadInstallSnapshot(ref payloadReader),
            RaftMessageType.InstallSnapshotResponse => ReadInstallSnapshotResponse(ref payloadReader),
            _ => null
         };

         if (payload == null)
         {
            reader.Advance(1);
            continue;
         }

         return true;
      }

      return false;
   }

   public static void WriteRequestVote(IBufferWriter<byte> writer, RequestVoteRequest request)
   {
      var candidateIdBytes = Encoding.UTF8.GetBytes(request.CandidateId);
      if (candidateIdBytes.Length > ushort.MaxValue)
      {
         throw new ArgumentOutOfRangeException(nameof(request.CandidateId), "CandidateId exceeds maximum allowed length of 65535 bytes.");
      }

      var payloadLength = 8 + 2 + candidateIdBytes.Length + 8 + 8;

      WriteHeader(writer, RaftMessageType.RequestVote, payloadLength);

      var span = writer.GetSpan(payloadLength);
      BinaryPrimitives.WriteUInt64LittleEndian(span[..8], request.Term);
      BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(8, 2), (ushort)candidateIdBytes.Length);
      candidateIdBytes.CopyTo(span.Slice(10, candidateIdBytes.Length));

      var offset = 10 + candidateIdBytes.Length;
      BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(offset, 8), request.LastLogIndex);
      BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(offset + 8, 8), request.LastLogTerm);

      writer.Advance(payloadLength);
   }

   public static void WriteRequestVoteResponse(IBufferWriter<byte> writer, RequestVoteResponse response)
   {
      const int payloadLength = 8 + 1;
      WriteHeader(writer, RaftMessageType.RequestVoteResponse, payloadLength);

      var span = writer.GetSpan(payloadLength);
      BinaryPrimitives.WriteUInt64LittleEndian(span[..8], response.Term);
      span[8] = response.VoteGranted ? (byte)1 : (byte)0;

      writer.Advance(payloadLength);
   }

   public static void WriteAppendEntries(IBufferWriter<byte> writer, AppendEntriesRequest request)
   {
      var leaderIdBytes = Encoding.UTF8.GetBytes(request.LeaderId);
      if (leaderIdBytes.Length > ushort.MaxValue)
      {
         throw new ArgumentOutOfRangeException(nameof(request.LeaderId), "LeaderId exceeds maximum allowed length of 65535 bytes.");
      }

      var entriesLength = 0;
      var entries = request.Entries;

      for (var i = 0; i < entries.Count; i++)
      {
         entriesLength += 8 + 8 + 4 + entries[i].Data.Length;
      }

      var payloadLength = 8 + 2 + leaderIdBytes.Length + 8 + 8 + 8 + 4 + entriesLength;
      WriteHeader(writer, RaftMessageType.AppendEntries, payloadLength);

      var span = writer.GetSpan(payloadLength);
      BinaryPrimitives.WriteUInt64LittleEndian(span[..8], request.Term);
      BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(8, 2), (ushort)leaderIdBytes.Length);
      leaderIdBytes.CopyTo(span.Slice(10, leaderIdBytes.Length));

      var offset = 10 + leaderIdBytes.Length;
      BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(offset, 8), request.PrevLogIndex);
      BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(offset + 8, 8), request.PrevLogTerm);
      BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(offset + 16, 8), request.LeaderCommitIndex);
      BinaryPrimitives.WriteInt32LittleEndian(span.Slice(offset + 24, 4), entries.Count);
      offset += 28;

      for (var i = 0; i < entries.Count; i++)
      {
         var entry = entries[i];
         BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(offset, 8), entry.Term);
         BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(offset + 8, 8), entry.Index);
         BinaryPrimitives.WriteInt32LittleEndian(span.Slice(offset + 16, 4), entry.Data.Length);
         entry.Data.Span.CopyTo(span.Slice(offset + 20, entry.Data.Length));
         offset += 20 + entry.Data.Length;
      }

      writer.Advance(payloadLength);
   }

   public static void WriteAppendEntriesResponse(IBufferWriter<byte> writer, AppendEntriesResponse response)
   {
      const int payloadLength = 8 + 1 + 8;
      WriteHeader(writer, RaftMessageType.AppendEntriesResponse, payloadLength);

      var span = writer.GetSpan(payloadLength);
      BinaryPrimitives.WriteUInt64LittleEndian(span[..8], response.Term);
      span[8] = response.Success ? (byte)1 : (byte)0;
      BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(9, 8), response.MatchIndex);

      writer.Advance(payloadLength);
   }

   public static void WriteInstallSnapshot(IBufferWriter<byte> writer, InstallSnapshotRequest request)
   {
      var leaderIdBytes = Encoding.UTF8.GetBytes(request.LeaderId);
      if (leaderIdBytes.Length > ushort.MaxValue)
      {
         throw new ArgumentOutOfRangeException(nameof(request.LeaderId), "LeaderId exceeds maximum allowed length of 65535 bytes.");
      }

      var payloadLength = 8 + 2 + leaderIdBytes.Length + 8 + 8 + 4 + request.Data.Length;
      WriteHeader(writer, RaftMessageType.InstallSnapshot, payloadLength);

      var span = writer.GetSpan(payloadLength);
      BinaryPrimitives.WriteUInt64LittleEndian(span[..8], request.Term);
      BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(8, 2), (ushort)leaderIdBytes.Length);
      leaderIdBytes.CopyTo(span.Slice(10, leaderIdBytes.Length));

      var offset = 10 + leaderIdBytes.Length;
      BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(offset, 8), request.LastIncludedIndex);
      BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(offset + 8, 8), request.LastIncludedTerm);
      BinaryPrimitives.WriteInt32LittleEndian(span.Slice(offset + 16, 4), request.Data.Length);
      request.Data.Span.CopyTo(span.Slice(offset + 20, request.Data.Length));

      writer.Advance(payloadLength);
   }

   public static void WriteInstallSnapshotResponse(IBufferWriter<byte> writer, InstallSnapshotResponse response)
   {
      const int payloadLength = 8 + 1;
      WriteHeader(writer, RaftMessageType.InstallSnapshotResponse, payloadLength);

      var span = writer.GetSpan(payloadLength);
      BinaryPrimitives.WriteUInt64LittleEndian(span[..8], response.Term);
      span[8] = response.Success ? (byte)1 : (byte)0;

      writer.Advance(payloadLength);
   }

   private static void WriteHeader(IBufferWriter<byte> writer, RaftMessageType type, int payloadLength)
   {
      var span = writer.GetSpan(HeaderSize);
      span[0] = MagicByte1;
      span[1] = MagicByte2;
      span[2] = CurrentVersion;
      span[3] = (byte)type;
      BinaryPrimitives.WriteInt32LittleEndian(span.Slice(4, 4), payloadLength);
      writer.Advance(HeaderSize);
   }

   private static RequestVoteRequest? ReadRequestVote(ref SequenceReader<byte> reader)
   {
      if (!reader.TryReadLittleEndian(out long termRaw) ||
          !reader.TryReadLittleEndian(out short candidateIdLengthRaw) ||
          candidateIdLengthRaw < 0 ||
          reader.Remaining < (ushort)candidateIdLengthRaw + 16)
      {
         return null;
      }

      var candidateIdLength = (ushort)candidateIdLengthRaw;
      var candidateIdBytes = new byte[candidateIdLength];
      if (!reader.TryCopyTo(candidateIdBytes))
      {
         return null;
      }
      reader.Advance(candidateIdLength);

      if (!reader.TryReadLittleEndian(out long lastLogIndexRaw) ||
          !reader.TryReadLittleEndian(out long lastLogTermRaw))
      {
         return null;
      }

      return new RequestVoteRequest
      {
         Term = (ulong)termRaw,
         CandidateId = Encoding.UTF8.GetString(candidateIdBytes),
         LastLogIndex = (ulong)lastLogIndexRaw,
         LastLogTerm = (ulong)lastLogTermRaw
      };
   }

   private static RequestVoteResponse? ReadRequestVoteResponse(ref SequenceReader<byte> reader)
   {
      if (!reader.TryReadLittleEndian(out long termRaw) || !reader.TryRead(out var voteGrantedByte))
      {
         return null;
      }

      return new RequestVoteResponse
      {
         Term = (ulong)termRaw,
         VoteGranted = voteGrantedByte != 0
      };
   }

   private static AppendEntriesRequest? ReadAppendEntries(ref SequenceReader<byte> reader)
   {
      if (!reader.TryReadLittleEndian(out long termRaw) ||
          !reader.TryReadLittleEndian(out short leaderIdLengthRaw) ||
          leaderIdLengthRaw < 0 ||
          reader.Remaining < (ushort)leaderIdLengthRaw + 28)
      {
         return null;
      }

      var leaderIdLength = (ushort)leaderIdLengthRaw;
      var leaderIdBytes = new byte[leaderIdLength];
      if (!reader.TryCopyTo(leaderIdBytes))
      {
         return null;
      }
      reader.Advance(leaderIdLength);

      if (!reader.TryReadLittleEndian(out long prevLogIndexRaw) ||
          !reader.TryReadLittleEndian(out long prevLogTermRaw) ||
          !reader.TryReadLittleEndian(out long leaderCommitIndexRaw) ||
          !reader.TryReadLittleEndian(out int entriesCount) ||
          entriesCount < 0)
      {
         return null;
      }

      var entries = new List<RaftLogEntry>(entriesCount);
      for (var i = 0; i < entriesCount; i++)
      {
         if (!reader.TryReadLittleEndian(out long entryTermRaw) ||
             !reader.TryReadLittleEndian(out long entryIndexRaw) ||
             !reader.TryReadLittleEndian(out int dataLength) ||
             dataLength < 0 ||
             reader.Remaining < dataLength)
         {
            return null;
         }

         var data = new byte[dataLength];
         if (!reader.TryCopyTo(data))
         {
            return null;
         }
         reader.Advance(dataLength);

         entries.Add(new RaftLogEntry((ulong)entryTermRaw, (ulong)entryIndexRaw, data));
      }

      return new AppendEntriesRequest
      {
         Term = (ulong)termRaw,
         LeaderId = Encoding.UTF8.GetString(leaderIdBytes),
         PrevLogIndex = (ulong)prevLogIndexRaw,
         PrevLogTerm = (ulong)prevLogTermRaw,
         LeaderCommitIndex = (ulong)leaderCommitIndexRaw,
         Entries = entries
      };
   }

   private static AppendEntriesResponse? ReadAppendEntriesResponse(ref SequenceReader<byte> reader)
   {
      if (!reader.TryReadLittleEndian(out long termRaw) ||
          !reader.TryRead(out var successByte) ||
          !reader.TryReadLittleEndian(out long matchIndexRaw))
      {
         return null;
      }

      return new AppendEntriesResponse
      {
         Term = (ulong)termRaw,
         Success = successByte != 0,
         MatchIndex = (ulong)matchIndexRaw
      };
   }

   private static InstallSnapshotRequest? ReadInstallSnapshot(ref SequenceReader<byte> reader)
   {
      if (!reader.TryReadLittleEndian(out long termRaw) ||
          !reader.TryReadLittleEndian(out short leaderIdLengthRaw) ||
          leaderIdLengthRaw < 0 ||
          reader.Remaining < (ushort)leaderIdLengthRaw + 20)
      {
         return null;
      }

      var leaderIdLength = (ushort)leaderIdLengthRaw;
      var leaderIdBytes = new byte[leaderIdLength];
      if (!reader.TryCopyTo(leaderIdBytes))
      {
         return null;
      }
      reader.Advance(leaderIdLength);

      if (!reader.TryReadLittleEndian(out long lastIncludedIndexRaw) ||
          !reader.TryReadLittleEndian(out long lastIncludedTermRaw) ||
          !reader.TryReadLittleEndian(out int dataLength) ||
          dataLength < 0 ||
          reader.Remaining < dataLength)
      {
         return null;
      }

      var data = new byte[dataLength];
      if (!reader.TryCopyTo(data))
      {
         return null;
      }
      reader.Advance(dataLength);

      return new InstallSnapshotRequest
      {
         Term = (ulong)termRaw,
         LeaderId = Encoding.UTF8.GetString(leaderIdBytes),
         LastIncludedIndex = (ulong)lastIncludedIndexRaw,
         LastIncludedTerm = (ulong)lastIncludedTermRaw,
         Data = data
      };
   }

   private static InstallSnapshotResponse? ReadInstallSnapshotResponse(ref SequenceReader<byte> reader)
   {
      if (!reader.TryReadLittleEndian(out long termRaw) || !reader.TryRead(out var successByte))
      {
         return null;
      }

      return new InstallSnapshotResponse
      {
         Term = (ulong)termRaw,
         Success = successByte != 0
      };
   }
}
