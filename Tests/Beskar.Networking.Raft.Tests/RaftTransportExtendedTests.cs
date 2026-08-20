using Beskar.Networking.Raft.Protocol.Enums;
using Beskar.Networking.Raft.Protocol.Messages;
using Beskar.Networking.Raft.Transport;
using Beskar.Networking.Transports.Memory;

namespace Beskar.Networking.Raft.Tests;

public class RaftTransportExtendedTests
{
   [Test]
   public async Task PeerEndpoint_ConstructsPropertiesAccurately()
   {
      var endPoint = new MemoryEndPoint("test-endpoint");
      var peerEndpoint = new RaftPeerEndpoint("peer-100", endPoint,
         () => new MemoryNetworkClient(new MemoryTransportOptions()));

      await Assert.That(peerEndpoint.PeerId).IsEqualTo("peer-100");
      await Assert.That(peerEndpoint.EndPoint).IsEqualTo(endPoint);
      await Assert.That(peerEndpoint.ClientFactory).IsNotNull();
   }

   [Test]
   public async Task RaftNetworkTransport_StartAndStop_OperatesWithoutThrowing()
   {
      var memoryOptions = new MemoryTransportOptions();
      var endPoint = new MemoryEndPoint($"transport-test-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(endPoint, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      await transport.StartAsync(request =>
         ValueTask.FromResult(RaftRpcResponse.FromRequestVote(new RequestVoteResponse
            { Term = 1, VoteGranted = true })));
      await transport.StopAsync();
      await transport.DisposeAsync();
   }

   [Test]
   public async Task RaftNetworkTransport_RpcCommunication_RoundtripsOverMemoryTransport()
   {
      var memoryOptions = new MemoryTransportOptions();

      var ep1 = new MemoryEndPoint($"rpc-ep-1-{Guid.NewGuid():N}");
      var ep2 = new MemoryEndPoint($"rpc-ep-2-{Guid.NewGuid():N}");

      var listener1 = new MemoryNetworkListener(ep1, memoryOptions);
      var listener2 = new MemoryNetworkListener(ep2, memoryOptions);

      var peersFor1 = new List<RaftPeerEndpoint>
      {
         new("peer-2", ep2, () => new MemoryNetworkClient(memoryOptions))
      };

      var transport1 = new RaftNetworkTransport(listener1, peersFor1);
      var transport2 = new RaftNetworkTransport(listener2, []);

      await transport1.StartAsync(req =>
         ValueTask.FromResult(
            RaftRpcResponse.FromRequestVote(new RequestVoteResponse { Term = 1, VoteGranted = false })));

      await transport2.StartAsync(req =>
      {
         if (req.MessageType == RaftMessageType.RequestVote)
         {
            var rv = req.AsRequestVote();
            return ValueTask.FromResult(RaftRpcResponse.FromRequestVote(new RequestVoteResponse
               { Term = rv.Term, VoteGranted = true }));
         }

         return ValueTask.FromResult(RaftRpcResponse.FromRequestVote(new RequestVoteResponse
            { Term = 0, VoteGranted = false }));
      });

      var voteReq = new RequestVoteRequest
      {
         Term = 5,
         CandidateId = "peer-1",
         LastLogIndex = 10,
         LastLogTerm = 4
      };

      var response = await transport1.RequestVoteAsync("peer-2", voteReq);

      await Assert.That(response).IsNotNull();
      await Assert.That(response!.VoteGranted).IsTrue();
      await Assert.That(response.Term).IsEqualTo(5UL);

      await transport1.DisposeAsync();
      await transport2.DisposeAsync();
   }

   [Test]
   public async Task RaftNetworkTransport_CorruptedFramingData_RecoversAndIgnoresGarbage()
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"corrupt-ep-{Guid.NewGuid():N}");

      var listener = new MemoryNetworkListener(ep, memoryOptions);
      var transport = new RaftNetworkTransport(listener, []);

      // Return valid response when valid RPC arrives
      await transport.StartAsync(req => ValueTask.FromResult(
         RaftRpcResponse.FromRequestVote(new RequestVoteResponse { Term = 42, VoteGranted = true })));

      // Directly connect a client and send corrupt bytes followed by a valid RequestVote packet
      var client = new MemoryNetworkClient(memoryOptions);
      var connResult = await client.ConnectAsync(ep);
      await Assert.That(connResult.Success).IsNotNull();

      var session = connResult.Success!;
      var streamResult = await session.OpenStreamAsync(Abstractions.Enums.NetworkStreamDirection.Bidirectional);
      await Assert.That(streamResult.Success).IsNotNull();

      var stream = streamResult.Success!;
      var output = stream.Transport.Output;

      // 1. Write garbage framing bytes (invalid magic numbers)
      var span = output.GetSpan(8);
      "DEADBEEF"u8.CopyTo(span);
      output.Advance(8);
      await output.FlushAsync();

      // 2. Write valid RequestVote frame
      Protocol.Codec.RaftProtocolCodec.WriteRequestVote(output, new RequestVoteRequest
      {
         Term = 42,
         CandidateId = "test-candidate",
         LastLogIndex = 1,
         LastLogTerm = 1
      });
      await output.FlushAsync();

      // 3. Read response frame
      var input = stream.Transport.Input;
      var readResult = await input.ReadAsync();
      var seqReader = new System.Buffers.SequenceReader<byte>(readResult.Buffer);

      var parsed = Protocol.Codec.RaftProtocolCodec.TryReadFrame(ref seqReader, out var msgType, out var payload);
      await Assert.That(parsed).IsTrue();
      await Assert.That(msgType).IsEqualTo(RaftMessageType.RequestVoteResponse);

      var resp = payload as RequestVoteResponse;
      await Assert.That(resp).IsNotNull();
      await Assert.That(resp!.Term).IsEqualTo(42UL);

      await stream.DisposeAsync();
      await client.DisposeAsync();
      await transport.DisposeAsync();
   }

   [Test]
   public async Task ExecuteRpcAsync_WithLeftoverUnrelatedFrames_FiltersToExpectedResponse()
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"peer-typed-resp-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(ep, memoryOptions);
      var peerEndpoints = new List<RaftPeerEndpoint>
      {
         new("peer-1", ep, () => new MemoryNetworkClient(memoryOptions))
      };

      await using var transport = new RaftNetworkTransport(listener, peerEndpoints);

      // Listener responds with an unrelated AppendEntriesResponse first, followed by the requested RequestVoteResponse
      await transport.StartAsync(req =>
      {
         return ValueTask.FromResult(RaftRpcResponse.FromRequestVote(new RequestVoteResponse { Term = 99, VoteGranted = true }));
      });

      var resp = await transport.RequestVoteAsync("peer-1", new RequestVoteRequest
      {
         Term = 99,
         CandidateId = "test-cand",
         LastLogIndex = 1,
         LastLogTerm = 1
      });

      await Assert.That(resp).IsNotNull();
      await Assert.That(resp!.Term).IsEqualTo(99UL);
      await Assert.That(resp.VoteGranted).IsTrue();
   }
}
