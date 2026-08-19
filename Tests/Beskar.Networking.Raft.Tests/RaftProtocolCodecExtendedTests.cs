using System.Buffers;
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
}
