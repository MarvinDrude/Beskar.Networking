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
}
