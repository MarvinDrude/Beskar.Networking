using System.Buffers;
using System.Text;
using Beskar.Networking.Raft.Protocol.Codec;
using Beskar.Networking.Raft.Protocol.Enums;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;

namespace Beskar.Networking.Raft.Tests;

public class ProtocolCodecTests
{
   [Test]
   public async Task RequestVote_Roundtrip_DecodesAccurately()
   {
      var original = new RequestVoteRequest
      {
         Term = 42,
         CandidateId = "node-alpha-1",
         LastLogIndex = 105,
         LastLogTerm = 41
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteRequestVote(buffer, original);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.RequestVote);
      await Assert.That(payload).IsTypeOf<RequestVoteRequest>();

      var decoded = (RequestVoteRequest)payload!;
      await Assert.That(decoded.Term).IsEqualTo(original.Term);
      await Assert.That(decoded.CandidateId).IsEqualTo(original.CandidateId);
      await Assert.That(decoded.LastLogIndex).IsEqualTo(original.LastLogIndex);
      await Assert.That(decoded.LastLogTerm).IsEqualTo(original.LastLogTerm);
   }

   [Test]
   public async Task RequestVoteResponse_Roundtrip_DecodesAccurately()
   {
      var original = new RequestVoteResponse
      {
         Term = 99,
         VoteGranted = true
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteRequestVoteResponse(buffer, original);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.RequestVoteResponse);

      var decoded = (RequestVoteResponse)payload!;
      await Assert.That(decoded.Term).IsEqualTo(original.Term);
      await Assert.That(decoded.VoteGranted).IsTrue();
   }

   [Test]
   public async Task AppendEntries_WithEntries_Roundtrip_DecodesAccurately()
   {
      var original = new AppendEntriesRequest
      {
         Term = 7,
         LeaderId = "node-leader-01",
         PrevLogIndex = 12,
         PrevLogTerm = 6,
         LeaderCommitIndex = 10,
         Entries = new[]
         {
            new RaftLogEntry(7, 13, Encoding.UTF8.GetBytes("SET key1 value1")),
            new RaftLogEntry(7, 14, Encoding.UTF8.GetBytes("DEL key2"))
         }
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteAppendEntries(buffer, original);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.AppendEntries);

      var decoded = (AppendEntriesRequest)payload!;
      await Assert.That(decoded.Term).IsEqualTo(original.Term);
      await Assert.That(decoded.LeaderId).IsEqualTo(original.LeaderId);
      await Assert.That(decoded.PrevLogIndex).IsEqualTo(original.PrevLogIndex);
      await Assert.That(decoded.PrevLogTerm).IsEqualTo(original.PrevLogTerm);
      await Assert.That(decoded.LeaderCommitIndex).IsEqualTo(original.LeaderCommitIndex);
      await Assert.That(decoded.Entries.Count).IsEqualTo(2);

      await Assert.That(decoded.Entries[0].Term).IsEqualTo(7UL);
      await Assert.That(decoded.Entries[0].Index).IsEqualTo(13UL);
      await Assert.That(Encoding.UTF8.GetString(decoded.Entries[0].Data.Span)).IsEqualTo("SET key1 value1");

      await Assert.That(decoded.Entries[1].Term).IsEqualTo(7UL);
      await Assert.That(decoded.Entries[1].Index).IsEqualTo(14UL);
      await Assert.That(Encoding.UTF8.GetString(decoded.Entries[1].Data.Span)).IsEqualTo("DEL key2");
   }

   [Test]
   public async Task AppendEntriesResponse_Roundtrip_DecodesAccurately()
   {
      var original = new AppendEntriesResponse
      {
         Term = 15,
         Success = true,
         MatchIndex = 300
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteAppendEntriesResponse(buffer, original);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.AppendEntriesResponse);

      var decoded = (AppendEntriesResponse)payload!;
      await Assert.That(decoded.Term).IsEqualTo(original.Term);
      await Assert.That(decoded.Success).IsTrue();
      await Assert.That(decoded.MatchIndex).IsEqualTo(300UL);
   }

   [Test]
   public async Task InstallSnapshot_Roundtrip_DecodesAccurately()
   {
      var snapshotData = Encoding.UTF8.GetBytes("FULL_CLUSTER_STATE_DUMP_12345");
      var original = new InstallSnapshotRequest
      {
         Term = 20,
         LeaderId = "leader-3",
         LastIncludedIndex = 500,
         LastIncludedTerm = 19,
         Data = snapshotData
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteInstallSnapshot(buffer, original);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.InstallSnapshot);

      var decoded = (InstallSnapshotRequest)payload!;
      await Assert.That(decoded.Term).IsEqualTo(original.Term);
      await Assert.That(decoded.LeaderId).IsEqualTo(original.LeaderId);
      await Assert.That(decoded.LastIncludedIndex).IsEqualTo(500UL);
      await Assert.That(decoded.LastIncludedTerm).IsEqualTo(19UL);
      await Assert.That(Encoding.UTF8.GetString(decoded.Data.Span)).IsEqualTo("FULL_CLUSTER_STATE_DUMP_12345");
   }
}
