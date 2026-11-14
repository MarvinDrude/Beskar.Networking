using System.Buffers;
using System.Buffers.Binary;
using System.Text;
using Beskar.Networking.Raft.Protocol.Codec;
using Beskar.Networking.Raft.Protocol.Enums;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;

namespace Beskar.Networking.Raft.Tests;

public class RaftProtocolCodecExtendedTests
{
   [Test]
   public async Task TryReadFrame_CorruptMagicBytes_ReturnsFalse()
   {
      var corruptData = new byte[] { 0xDE, 0xAD, (byte)RaftMessageType.RequestVote, 0, 0, 0, 0 };
      var sequence = new ReadOnlySequence<byte>(corruptData);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var messageType, out var payload);

      await Assert.That(success).IsFalse();
   }

   [Test]
   public async Task TryReadFrame_IncompleteHeader_ReturnsFalse()
   {
      var shortHeader = new byte[] { 0xBE, 0x52, (byte)RaftMessageType.AppendEntries }; // Only 3 bytes
      var sequence = new ReadOnlySequence<byte>(shortHeader);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out _, out _);

      await Assert.That(success).IsFalse();
   }

   [Test]
   public async Task TryReadFrame_TruncatedPayloadLength_ReturnsFalse()
   {
      var buffer = new ArrayBufferWriter<byte>();
      var request = new RequestVoteRequest
      {
         Term = 1,
         CandidateId = "very-long-candidate-identifier-string-that-needs-bytes",
         LastLogIndex = 10,
         LastLogTerm = 1
      };
      RaftProtocolCodec.WriteRequestVote(buffer, request);

      // Truncate payload bytes by 10 bytes
      var truncatedMemory = buffer.WrittenMemory[..^10];
      var sequence = new ReadOnlySequence<byte>(truncatedMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out _, out _);

      await Assert.That(success).IsFalse();
   }

   [Test]
   public async Task TryReadFrame_UnknownMessageType_ReturnsFalse()
   {
      var buffer = new byte[] { 0xBE, 0x52, 0xFF, 0, 0, 0, 0 }; // 0xFF is invalid message type
      var sequence = new ReadOnlySequence<byte>(buffer);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out _, out _);

      await Assert.That(success).IsFalse();
   }

   [Test]
   [Arguments(0UL, "node-1", 0UL, 0UL)]
   [Arguments(18446744073709551615UL, "node-999", 18446744073709551615UL, 18446744073709551615UL)]
   public async Task RequestVote_ExtremeValues_RoundtripAccurately(
      ulong term, string candidateId, ulong lastIndex, ulong lastTerm)
   {
      var original = new RequestVoteRequest
      {
         Term = term,
         CandidateId = candidateId,
         LastLogIndex = lastIndex,
         LastLogTerm = lastTerm
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteRequestVote(buffer, original);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.RequestVote);
      var decoded = (RequestVoteRequest)payload!;
      await Assert.That(decoded.Term).IsEqualTo(term);
      await Assert.That(decoded.CandidateId).IsEqualTo(candidateId);
      await Assert.That(decoded.LastLogIndex).IsEqualTo(lastIndex);
      await Assert.That(decoded.LastLogTerm).IsEqualTo(lastTerm);
   }

   [Test]
   public async Task AppendEntries_EmptyHeartbeat_RoundtripAccurately()
   {
      var heartbeat = new AppendEntriesRequest
      {
         Term = 5,
         LeaderId = "leader-alpha",
         PrevLogIndex = 100,
         PrevLogTerm = 4,
         LeaderCommitIndex = 95,
         Entries = Array.Empty<RaftLogEntry>()
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteAppendEntries(buffer, heartbeat);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.AppendEntries);

      var decoded = (AppendEntriesRequest)payload!;
      await Assert.That(decoded.Entries.Count).IsEqualTo(0);
   }

   [Test]
   public async Task AppendEntries_LargeBatch_RoundtripAccurately()
   {
      var entries = new List<RaftLogEntry>();
      for (ulong i = 1; i <= 50; i++)
         entries.Add(new RaftLogEntry(10, i, Encoding.UTF8.GetBytes($"COMMAND_DATA_PAYLOAD_{i}")));

      var request = new AppendEntriesRequest
      {
         Term = 10,
         LeaderId = "leader-batch",
         PrevLogIndex = 0,
         PrevLogTerm = 0,
         LeaderCommitIndex = 25,
         Entries = entries
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteAppendEntries(buffer, request);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      var decoded = (AppendEntriesRequest)payload!;
      await Assert.That(decoded.Entries.Count).IsEqualTo(50);
      await Assert.That(Encoding.UTF8.GetString(decoded.Entries[49].Data.Span)).IsEqualTo("COMMAND_DATA_PAYLOAD_50");
   }

   [Test]
   [Arguments(true)]
   [Arguments(false)]
   public async Task InstallSnapshotResponse_RoundtripAccurately(bool successFlag)
   {
      var response = new InstallSnapshotResponse
      {
         Term = 88,
         Success = successFlag
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteInstallSnapshotResponse(buffer, response);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.InstallSnapshotResponse);

      var decoded = (InstallSnapshotResponse)payload!;
      await Assert.That(decoded.Term).IsEqualTo(88UL);
      await Assert.That(decoded.Success).IsEqualTo(successFlag);
   }

   [Test]
   public async Task TryReadFrame_CorruptedPayload_AllowsReadingSubsequentValidFrame()
   {
      var writer = new ArrayBufferWriter<byte>();

      // Frame 1: Valid header with unknown message type (0xEE) and payload
      writer.GetSpan(12)[0] = 0xBE;
      writer.GetSpan(12)[1] = 0x52;
      writer.GetSpan(12)[2] = 0x01; // Version
      writer.GetSpan(12)[3] = 0xEE; // Unknown type
      BinaryPrimitives.WriteInt32LittleEndian(writer.GetSpan(12).Slice(4, 4), 4);
      "JUNK"u8.CopyTo(writer.GetSpan(12).Slice(8, 4));
      writer.Advance(12);

      // Frame 2: Valid RequestVoteResponse
      var validResponse = new RequestVoteResponse { Term = 77, VoteGranted = true };
      RaftProtocolCodec.WriteRequestVoteResponse(writer, validResponse);

      var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      // Attempt to read frames
      var foundValid = false;
      while (RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload))
         if (type == RaftMessageType.RequestVoteResponse && payload is RequestVoteResponse rvr && rvr.Term == 77)
         {
            foundValid = true;
            break;
         }

      await Assert.That(foundValid).IsTrue();
   }

   [Test]
   public async Task Codec_LargeCandidateIdOver32KB_RoundtripsAccurately()
   {
      var largeCandidateId = new string('c', 40000); // 40,000 bytes > 32767 (short.MaxValue)
      var request = new RequestVoteRequest
      {
         Term = 5,
         CandidateId = largeCandidateId,
         LastLogIndex = 100,
         LastLogTerm = 4
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteRequestVote(buffer, request);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.RequestVote);
      var decoded = (RequestVoteRequest)payload!;
      await Assert.That(decoded.CandidateId.Length).IsEqualTo(40000);
      await Assert.That(decoded.CandidateId).IsEqualTo(largeCandidateId);
   }

   [Test]
   [Timeout(5000)]
   public async Task TryReadFrame_AppendEntriesWithImpossibleEntriesCount_SkipsCorruptFrameWithoutAllocating(
      CancellationToken ct)
   {
      var writer = new ArrayBufferWriter<byte>();

      // Frame 1: AppendEntries header with payload length 38, but entriesCount = int.MaxValue
      var span = writer.GetSpan(8 + 38);
      span[0] = RaftProtocolCodec.MagicByte1;
      span[1] = RaftProtocolCodec.MagicByte2;
      span[2] = RaftProtocolCodec.CurrentVersion;
      span[3] = (byte)RaftMessageType.AppendEntries;
      BinaryPrimitives.WriteInt32LittleEndian(span.Slice(4, 4), 38);
      BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(8, 8), 1); // Term
      BinaryPrimitives.WriteUInt16LittleEndian(span.Slice(16, 2), 0); // LeaderIdLen = 0
      BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(18, 8), 0); // PrevLogIndex
      BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(26, 8), 0); // PrevLogTerm
      BinaryPrimitives.WriteUInt64LittleEndian(span.Slice(34, 8), 0); // LeaderCommitIndex
      BinaryPrimitives.WriteInt32LittleEndian(span.Slice(42, 4), int.MaxValue); // EntriesCount
      writer.Advance(8 + 38);

      // Frame 2: Valid RequestVoteResponse
      RaftProtocolCodec.WriteRequestVoteResponse(writer, new RequestVoteResponse { Term = 12, VoteGranted = true });

      var sequence = new ReadOnlySequence<byte>(writer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.RequestVoteResponse);
      var resp = payload as RequestVoteResponse;
      await Assert.That(resp).IsNotNull();
      await Assert.That(resp!.Term).IsEqualTo(12UL);
      await Assert.That(resp.VoteGranted).IsTrue();
   }

   [Test]
   [Timeout(5000)]
   public async Task TryReadFrame_RequestVoteTruncatedBeforePreVoteFlag_ReturnsFalse(CancellationToken ct)
   {
      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteRequestVote(buffer, new RequestVoteRequest
      {
         Term = 3,
         CandidateId = "node-1",
         LastLogIndex = 5,
         LastLogTerm = 2,
         IsPreVote = true
      });

      // Copy frame excluding the final IsPreVote byte and adjust header payloadLength accordingly
      var bytes = buffer.WrittenSpan.ToArray();
      var origPayloadLen = BinaryPrimitives.ReadInt32LittleEndian(bytes.AsSpan(4, 4));
      var truncated = bytes[..^1];
      BinaryPrimitives.WriteInt32LittleEndian(truncated.AsSpan(4, 4), origPayloadLen - 1);

      var sequence = new ReadOnlySequence<byte>(truncated);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out _, out _);

      await Assert.That(success).IsFalse();
   }
}
