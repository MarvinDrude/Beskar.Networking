using System.Buffers;
using System.Text;
using Beskar.Networking.Raft.Protocol.Codec;
using Beskar.Networking.Raft.Protocol.Enums;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Protocol.Models;

namespace Beskar.Networking.Raft.Tests;

public class RaftProtocolCodecMatrixTests
{
   [Test]
   [Arguments(1UL, "node-1", 1UL, 1UL)]
   [Arguments(2UL, "node-2", 10UL, 1UL)]
   [Arguments(3UL, "candidate-x", 15UL, 2UL)]
   [Arguments(4UL, "candidate-y", 20UL, 3UL)]
   [Arguments(5UL, "candidate-a", 100UL, 4UL)]
   [Arguments(10UL, "candidate-b", 500UL, 9UL)]
   [Arguments(50UL, "candidate-z", 750UL, 49UL)]
   [Arguments(100UL, "node-c-region-us", 1000UL, 99UL)]
   [Arguments(500UL, "node-d-eu-west", 2500UL, 499UL)]
   [Arguments(1000UL, "node-d", 5000UL, 999UL)]
   [Arguments(50000UL, "node-e", 100000UL, 49999UL)]
   public async Task RequestVoteRequest_MatrixTest(
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
   [Arguments(1UL, true)]
   [Arguments(1UL, false)]
   [Arguments(5UL, true)]
   [Arguments(5UL, false)]
   [Arguments(100UL, true)]
   [Arguments(100UL, false)]
   [Arguments(99999UL, true)]
   [Arguments(99999UL, false)]
   public async Task RequestVoteResponse_MatrixTest(ulong term, bool voteGranted)
   {
      var original = new RequestVoteResponse
      {
         Term = term,
         VoteGranted = voteGranted
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteRequestVoteResponse(buffer, original);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.RequestVoteResponse);

      var decoded = (RequestVoteResponse)payload!;
      await Assert.That(decoded.Term).IsEqualTo(term);
      await Assert.That(decoded.VoteGranted).IsEqualTo(voteGranted);
   }

   [Test]
   [Arguments(1UL, "leader-1", 0UL, 0UL, 0UL, 0)]
   [Arguments(2UL, "leader-1", 1UL, 1UL, 1UL, 1)]
   [Arguments(5UL, "leader-alpha", 10UL, 4UL, 9UL, 5)]
   [Arguments(10UL, "leader-beta", 50UL, 9UL, 48UL, 10)]
   [Arguments(100UL, "leader-gamma", 200UL, 99UL, 195UL, 25)]
   public async Task AppendEntriesRequest_MatrixTest(
      ulong term, string leaderId, ulong prevIndex, ulong prevTerm, ulong commitIndex, int entryCount)
   {
      var entries = new List<RaftLogEntry>();
      for (var i = 1; i <= entryCount; i++)
         entries.Add(new RaftLogEntry(term, prevIndex + (ulong)i, Encoding.UTF8.GetBytes($"DATA_{i}")));

      var original = new AppendEntriesRequest
      {
         Term = term,
         LeaderId = leaderId,
         PrevLogIndex = prevIndex,
         PrevLogTerm = prevTerm,
         LeaderCommitIndex = commitIndex,
         Entries = entries
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteAppendEntries(buffer, original);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.AppendEntries);

      var decoded = (AppendEntriesRequest)payload!;
      await Assert.That(decoded.Term).IsEqualTo(term);
      await Assert.That(decoded.LeaderId).IsEqualTo(leaderId);
      await Assert.That(decoded.PrevLogIndex).IsEqualTo(prevIndex);
      await Assert.That(decoded.PrevLogTerm).IsEqualTo(prevTerm);
      await Assert.That(decoded.LeaderCommitIndex).IsEqualTo(commitIndex);
      await Assert.That(decoded.Entries.Count).IsEqualTo(entryCount);
   }

   [Test]
   [Arguments(1UL, true, 1UL)]
   [Arguments(1UL, false, 0UL)]
   [Arguments(2UL, true, 5UL)]
   [Arguments(2UL, false, 1UL)]
   [Arguments(5UL, true, 50UL)]
   [Arguments(5UL, false, 45UL)]
   [Arguments(10UL, true, 200UL)]
   [Arguments(10UL, false, 180UL)]
   [Arguments(100UL, true, 1000UL)]
   [Arguments(100UL, false, 950UL)]
   public async Task AppendEntriesResponse_MatrixTest(ulong term, bool success, ulong matchIndex)
   {
      var original = new AppendEntriesResponse
      {
         Term = term,
         Success = success,
         MatchIndex = matchIndex
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteAppendEntriesResponse(buffer, original);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var readSuccess = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(readSuccess).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.AppendEntriesResponse);

      var decoded = (AppendEntriesResponse)payload!;
      await Assert.That(decoded.Term).IsEqualTo(term);
      await Assert.That(decoded.Success).IsEqualTo(success);
      await Assert.That(decoded.MatchIndex).IsEqualTo(matchIndex);
   }

   [Test]
   [Arguments(1UL, "leader-1", 100UL, 1UL, 10)]
   [Arguments(2UL, "leader-1b", 250UL, 2UL, 50)]
   [Arguments(5UL, "leader-2", 500UL, 4UL, 100)]
   [Arguments(7UL, "leader-2b", 750UL, 6UL, 500)]
   [Arguments(10UL, "leader-3", 1000UL, 9UL, 1000)]
   [Arguments(50UL, "leader-4", 5000UL, 49UL, 5000)]
   public async Task InstallSnapshotRequest_MatrixTest(
      ulong term, string leaderId, ulong lastIndex, ulong lastTerm, int payloadBytes)
   {
      var dummyPayload = new byte[payloadBytes];
      Random.Shared.NextBytes(dummyPayload);

      var original = new InstallSnapshotRequest
      {
         Term = term,
         LeaderId = leaderId,
         LastIncludedIndex = lastIndex,
         LastIncludedTerm = lastTerm,
         Data = dummyPayload
      };

      var buffer = new ArrayBufferWriter<byte>();
      RaftProtocolCodec.WriteInstallSnapshot(buffer, original);

      var sequence = new ReadOnlySequence<byte>(buffer.WrittenMemory);
      var reader = new SequenceReader<byte>(sequence);

      var success = RaftProtocolCodec.TryReadFrame(ref reader, out var type, out var payload);

      await Assert.That(success).IsTrue();
      await Assert.That(type).IsEqualTo(RaftMessageType.InstallSnapshot);

      var decoded = (InstallSnapshotRequest)payload!;
      await Assert.That(decoded.Term).IsEqualTo(term);
      await Assert.That(decoded.LeaderId).IsEqualTo(leaderId);
      await Assert.That(decoded.LastIncludedIndex).IsEqualTo(lastIndex);
      await Assert.That(decoded.LastIncludedTerm).IsEqualTo(lastTerm);
      await Assert.That(decoded.Data.Length).IsEqualTo(payloadBytes);
   }
}
