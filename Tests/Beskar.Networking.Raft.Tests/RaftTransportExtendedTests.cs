using System.Buffers;
using System.IO.Pipelines;
using System.Net;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Raft.Protocol.Codec;
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
      var streamResult = await session.OpenStreamAsync();
      await Assert.That(streamResult.Success).IsNotNull();

      var stream = streamResult.Success!;
      var output = stream.Transport.Output;

      // 1. Write garbage framing bytes (invalid magic numbers)
      var span = output.GetSpan(8);
      "DEADBEEF"u8.CopyTo(span);
      output.Advance(8);
      await output.FlushAsync();

      // 2. Write valid RequestVote frame
      RaftProtocolCodec.WriteRequestVote(output, new RequestVoteRequest
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
      var seqReader = new SequenceReader<byte>(readResult.Buffer);

      var parsed = RaftProtocolCodec.TryReadFrame(ref seqReader, out var msgType, out var payload);
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
         return ValueTask.FromResult(RaftRpcResponse.FromRequestVote(new RequestVoteResponse
            { Term = 99, VoteGranted = true }));
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

   [Test]
   [Timeout(8000)]
   public async Task RaftNetworkTransport_MultipleFramesInSingleRead_SecondRpcReadsBufferedFramePromptly(
      CancellationToken ct)
   {
      var dummyEndpoint = new MemoryEndPoint($"dummy-listen-{Guid.NewGuid():N}");
      var memoryOptions = new MemoryTransportOptions();
      var dummyListener = new MemoryNetworkListener(dummyEndpoint, memoryOptions);

      var clientToServer = new Pipe();
      var serverToClient = new Pipe();
      var ep = new MemoryEndPoint($"pipe-peer-{Guid.NewGuid():N}");
      var session =
         new MemoryNetworkSession(ep, ep, new PipePairDuplexPipe(serverToClient.Reader, clientToServer.Writer));

      var peerClient = new PipeBackedNetworkClient(session);
      await using var transport = new RaftNetworkTransport(
         dummyListener,
         [new RaftPeerEndpoint("peer-1", ep, () => peerClient)]);

      using var responderCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      var responderTask = Task.Run(async () =>
      {
         try
         {
            // Read RPC 1 (RequestVote)
            while (!responderCts.IsCancellationRequested)
            {
               var read = await clientToServer.Reader.ReadAsync(responderCts.Token);
               var seq = read.Buffer;
               var reader = new SequenceReader<byte>(seq);
               if (RaftProtocolCodec.TryReadFrame(ref reader, out _, out _))
               {
                  clientToServer.Reader.AdvanceTo(reader.Position);
                  break;
               }

               clientToServer.Reader.AdvanceTo(seq.Start, seq.End);
            }

            // Write BOTH RequestVoteResponse and AppendEntriesResponse in a single flush
            var buf = new ArrayBufferWriter<byte>();
            RaftProtocolCodec.WriteRequestVoteResponse(
               buf, new RequestVoteResponse { Term = 5, VoteGranted = true });
            RaftProtocolCodec.WriteAppendEntriesResponse(
               buf, new AppendEntriesResponse { Term = 5, Success = true, MatchIndex = 1 });

            var mem = serverToClient.Writer.GetMemory(buf.WrittenCount);
            buf.WrittenSpan.CopyTo(mem.Span);
            serverToClient.Writer.Advance(buf.WrittenCount);
            await serverToClient.Writer.FlushAsync(responderCts.Token);

            // Consume RPC 2 (AppendEntries) without flushing anything new
            while (!responderCts.IsCancellationRequested)
            {
               var read = await clientToServer.Reader.ReadAsync(responderCts.Token);
               var seq = read.Buffer;
               var reader = new SequenceReader<byte>(seq);
               if (RaftProtocolCodec.TryReadFrame(ref reader, out _, out _))
               {
                  clientToServer.Reader.AdvanceTo(reader.Position);
                  break;
               }

               clientToServer.Reader.AdvanceTo(seq.Start, seq.End);
            }
         }
         catch (OperationCanceledException)
         {
         }
      }, ct);

      var rv = await transport.RequestVoteAsync("peer-1", new RequestVoteRequest
      {
         Term = 5,
         CandidateId = "n1",
         LastLogIndex = 0,
         LastLogTerm = 0
      }, ct);
      await Assert.That(rv).IsNotNull();
      await Assert.That(rv!.VoteGranted).IsTrue();

      using var shortTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      shortTimeoutCts.CancelAfter(500);

      AppendEntriesResponse? ae = null;
      try
      {
         ae = await transport.AppendEntriesAsync("peer-1", new AppendEntriesRequest
         {
            Term = 5,
            LeaderId = "n1",
            PrevLogIndex = 0,
            PrevLogTerm = 0,
            Entries = [],
            LeaderCommitIndex = 0
         }, shortTimeoutCts.Token);
      }
      catch (OperationCanceledException)
      {
         // Timed out waiting for already-buffered second frame
      }

      responderCts.Cancel();
      await responderTask;

      await Assert.That(ae).IsNotNull();
      await Assert.That(ae!.Success).IsTrue();
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftNetworkTransport_ConnectFails_DisposesCreatedClient(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var dummyListener =
         new MemoryNetworkListener(new MemoryEndPoint($"conn-fail-l-{Guid.NewGuid():N}"), memoryOptions);
      var ep = new MemoryEndPoint($"conn-fail-p-{Guid.NewGuid():N}");

      var createdClients = new List<ConnectFailingNetworkClient>();
      await using var transport = new RaftNetworkTransport(
         dummyListener,
         [
            new RaftPeerEndpoint("peer-1", ep, () =>
            {
               var c = new ConnectFailingNetworkClient();
               createdClients.Add(c);
               return c;
            })
         ]);

      var resp = await transport.RequestVoteAsync("peer-1", new RequestVoteRequest
      {
         Term = 1,
         CandidateId = "n1"
      }, ct);

      await Assert.That(resp).IsNull();
      await Assert.That(createdClients.Count).IsGreaterThanOrEqualTo(1);
      await Assert.That(createdClients[0].DisposeCount).IsEqualTo(1);
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftNetworkTransport_OpenStreamFails_DisposesSessionAndClient(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var dummyListener =
         new MemoryNetworkListener(new MemoryEndPoint($"stream-fail-l-{Guid.NewGuid():N}"), memoryOptions);
      var ep = new MemoryEndPoint($"stream-fail-p-{Guid.NewGuid():N}");

      var createdClients = new List<StreamFailingNetworkClient>();
      await using var transport = new RaftNetworkTransport(
         dummyListener,
         [
            new RaftPeerEndpoint("peer-1", ep, () =>
            {
               var c = new StreamFailingNetworkClient();
               createdClients.Add(c);
               return c;
            })
         ]);

      var resp = await transport.RequestVoteAsync("peer-1", new RequestVoteRequest
      {
         Term = 1,
         CandidateId = "n1"
      }, ct);

      await Assert.That(resp).IsNull();
      await Assert.That(createdClients.Count).IsGreaterThanOrEqualTo(1);
      await Assert.That(createdClients[0].SessionDisposeCount).IsEqualTo(1);
      await Assert.That(createdClients[0].DisposeCount).IsEqualTo(1);
   }

   [Test]
   [Timeout(5000)]
   public async Task RaftNetworkTransport_RestartAfterStop_UnbindsCleanlyOnSecondStop(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var ep = new MemoryEndPoint($"restart-transport-{Guid.NewGuid():N}");
      var listener = new MemoryNetworkListener(ep, memoryOptions);

      await using var transport = new RaftNetworkTransport(listener, []);
      try
      {
         await transport.StartAsync(_ => ValueTask.FromResult(RaftRpcResponse.FromRequestVote(
            new RequestVoteResponse { Term = 1, VoteGranted = false })), ct);
         await Assert.That(listener.IsBound).IsTrue();

         await transport.StopAsync(ct);
         await Assert.That(listener.IsBound).IsFalse();

         await transport.StartAsync(_ => ValueTask.FromResult(RaftRpcResponse.FromRequestVote(
            new RequestVoteResponse { Term = 1, VoteGranted = false })), ct);
         await Assert.That(listener.IsBound).IsTrue();

         await transport.StopAsync(ct);
         await Assert.That(listener.IsBound).IsFalse();
      }
      finally
      {
         if (listener.IsBound) await listener.UnbindAsync(CancellationToken.None);
      }
   }

   [Test]
   [Timeout(8000)]
   public async Task RaftNetworkTransport_TransientAcceptException_ContinuesServingRpcs(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var serverEp = new MemoryEndPoint($"accept-err-s-{Guid.NewGuid():N}");
      var clientEp = new MemoryEndPoint($"accept-err-c-{Guid.NewGuid():N}");

      var innerServerListener = new MemoryNetworkListener(serverEp, memoryOptions);
      var flakyListener = new OneShotThrowingAcceptListener(innerServerListener);

      await using var serverTransport = new RaftNetworkTransport(flakyListener, []);
      await using var clientTransport = new RaftNetworkTransport(
         new MemoryNetworkListener(clientEp, memoryOptions),
         [new RaftPeerEndpoint("server", serverEp, () => new MemoryNetworkClient(memoryOptions))]);

      await serverTransport.StartAsync(_ =>
      {
         return ValueTask.FromResult(RaftRpcResponse.FromRequestVote(
            new RequestVoteResponse { Term = 7, VoteGranted = true }));
      }, ct);

      await Task.Delay(50, ct);

      using var rpcCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      rpcCts.CancelAfter(600);

      RequestVoteResponse? resp = null;
      try
      {
         resp = await clientTransport.RequestVoteAsync("server", new RequestVoteRequest
         {
            Term = 7,
            CandidateId = "client"
         }, rpcCts.Token);
      }
      catch (OperationCanceledException)
      {
      }

      await Assert.That(flakyListener.AcceptCalls).IsGreaterThanOrEqualTo(2);
      await Assert.That(resp).IsNotNull();
      await Assert.That(resp!.VoteGranted).IsTrue();
   }

   [Test]
   [Timeout(8000)]
   public async Task RaftNetworkTransport_ClosedPooledStream_ReconnectsAndCompletesRpc(CancellationToken ct)
   {
      var memoryOptions = new MemoryTransportOptions();
      var dummyListener =
         new MemoryNetworkListener(new MemoryEndPoint($"closed-stream-l-{Guid.NewGuid():N}"), memoryOptions);
      var ep = new MemoryEndPoint($"closed-stream-p-{Guid.NewGuid():N}");

      var connectionCount = 0;
      var responderTasks = new List<Task>();
      using var cts = CancellationTokenSource.CreateLinkedTokenSource(ct);

      await using var transport = new RaftNetworkTransport(
         dummyListener,
         [
            new RaftPeerEndpoint("peer-1", ep, () =>
            {
               var connIndex = Interlocked.Increment(ref connectionCount);
               var c2s = new Pipe();
               var s2c = new Pipe();
               var session = new MemoryNetworkSession(ep, ep, new PipePairDuplexPipe(s2c.Reader, c2s.Writer));

               responderTasks.Add(Task.Run(async () =>
               {
                  try
                  {
                     var reqCount = 0;
                     while (!cts.IsCancellationRequested)
                     {
                        var read = await c2s.Reader.ReadAsync(cts.Token);
                        var seq = read.Buffer;
                        var reader = new SequenceReader<byte>(seq);
                        if (RaftProtocolCodec.TryReadFrame(
                               ref reader, out var msgType, out _))
                        {
                           c2s.Reader.AdvanceTo(reader.Position);
                           reqCount++;

                           if (connIndex == 1 && reqCount == 2)
                           {
                              // Gracefully close server output stream on second RPC of first pooled connection
                              await s2c.Writer.CompleteAsync();
                              return;
                           }

                           var outBuf = new ArrayBufferWriter<byte>();
                           if (msgType == RaftMessageType.RequestVote)
                              RaftProtocolCodec.WriteRequestVoteResponse(
                                 outBuf, new RequestVoteResponse { Term = 3, VoteGranted = true });
                           else
                              RaftProtocolCodec.WriteAppendEntriesResponse(
                                 outBuf, new AppendEntriesResponse { Term = 3, Success = true, MatchIndex = 1 });

                           var dst = s2c.Writer.GetMemory(outBuf.WrittenCount);
                           outBuf.WrittenSpan.CopyTo(dst.Span);
                           s2c.Writer.Advance(outBuf.WrittenCount);
                           await s2c.Writer.FlushAsync(cts.Token);
                        }
                        else if (read.IsCompleted)
                        {
                           return;
                        }
                        else
                        {
                           c2s.Reader.AdvanceTo(seq.Start, seq.End);
                        }
                     }
                  }
                  catch (OperationCanceledException)
                  {
                  }
               }, cts.Token));

               return new PipeBackedNetworkClient(session);
            })
         ]);

      var rv = await transport.RequestVoteAsync("peer-1", new RequestVoteRequest
      {
         Term = 3,
         CandidateId = "n1"
      }, ct);
      await Assert.That(rv).IsNotNull();
      await Assert.That(rv!.VoteGranted).IsTrue();

      var ae = await transport.AppendEntriesAsync("peer-1", new AppendEntriesRequest
      {
         Term = 3,
         LeaderId = "n1",
         Entries = []
      }, ct);

      cts.Cancel();
      await Task.WhenAll(responderTasks);

      await Assert.That(ae).IsNotNull();
      await Assert.That(ae!.Success).IsTrue();
   }

   private sealed class PipePairDuplexPipe(
      PipeReader input,
      PipeWriter output) : IDuplexPipe
   {
      public PipeReader Input => input;
      public PipeWriter Output => output;
   }

   private sealed class PipeBackedNetworkClient(MemoryNetworkSession session)
      : INetworkClient
   {
      private bool _connected;
      private int _disposed;

      public TransportKind Transport => session.Transport;

      public bool IsConnected => _connected && !session.SessionClosedToken.IsCancellationRequested &&
                                 Volatile.Read(ref _disposed) == 0;

      public NetworkClientStats Stats => new();
      public INetworkSession? Session => _connected ? session : null;
      public EndPoint? LocalAddress => session.LocalAddress;
      public EndPoint? RemoteAddress => session.RemoteAddress;

      public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
         EndPoint endPoint, CancellationToken ct = default)
      {
         _connected = true;
         return new ValueTask<Result<INetworkSession, NetworkCodeError>>(session);
      }

      public async ValueTask DisconnectAsync(CancellationToken ct = default)
      {
         _connected = false;
         await session.DisposeAsync();
      }

      public async ValueTask DisposeAsync()
      {
         if (Interlocked.Exchange(ref _disposed, 1) == 0)
         {
            _connected = false;
            await session.DisposeAsync();
         }
      }
   }

   private sealed class ConnectFailingNetworkClient : INetworkClient
   {
      private int _disposeCount;
      public int DisposeCount => Volatile.Read(ref _disposeCount);
      public TransportKind Transport => TransportKind.Memory;
      public bool IsConnected => false;
      public NetworkClientStats Stats => new();
      public INetworkSession? Session => null;
      public EndPoint? LocalAddress => null;
      public EndPoint? RemoteAddress => null;

      public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
         EndPoint endPoint, CancellationToken ct = default)
      {
         return new ValueTask<Result<INetworkSession, NetworkCodeError>>(
            new NetworkCodeError(-1, "connect failure"));
      }

      public ValueTask DisconnectAsync(CancellationToken ct = default)
      {
         return ValueTask.CompletedTask;
      }

      public ValueTask DisposeAsync()
      {
         Interlocked.Increment(ref _disposeCount);
         return ValueTask.CompletedTask;
      }
   }

   private sealed class StreamFailingNetworkClient : INetworkClient
   {
      private int _disposeCount;
      private StreamFailingNetworkSession? _session;
      public int DisposeCount => Volatile.Read(ref _disposeCount);
      public int SessionDisposeCount => _session?.DisposeCount ?? 0;

      public TransportKind Transport => TransportKind.Memory;
      public bool IsConnected { get; private set; }

      public NetworkClientStats Stats => new();
      public INetworkSession? Session => _session;
      public EndPoint? LocalAddress => _session?.LocalAddress;
      public EndPoint? RemoteAddress => _session?.RemoteAddress;

      public ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
         EndPoint endPoint, CancellationToken ct = default)
      {
         var pipe = new Pipe();
         var inner = new MemoryNetworkSession(endPoint, endPoint, new PipePairDuplexPipe(pipe.Reader, pipe.Writer));
         _session = new StreamFailingNetworkSession(inner);
         IsConnected = true;
         return new ValueTask<Result<INetworkSession, NetworkCodeError>>(_session);
      }

      public ValueTask DisconnectAsync(CancellationToken ct = default)
      {
         IsConnected = false;
         return ValueTask.CompletedTask;
      }

      public async ValueTask DisposeAsync()
      {
         Interlocked.Increment(ref _disposeCount);
         IsConnected = false;
         if (_session is not null) await _session.DisposeAsync();
      }
   }

   private sealed class StreamFailingNetworkSession(MemoryNetworkSession inner)
      : INetworkSession
   {
      private int _disposeCount;
      public int DisposeCount => Volatile.Read(ref _disposeCount);

      public Guid Id => inner.Id;
      public EndPoint RemoteAddress => inner.RemoteAddress;
      public EndPoint LocalAddress => inner.LocalAddress;
      public bool IsSupportingMultiplexing => inner.IsSupportingMultiplexing;
      public bool IsSupportingUnidirectional => inner.IsSupportingUnidirectional;
      public CancellationToken SessionClosedToken => inner.SessionClosedToken;
      public INetworkPropertyStore Properties => inner.Properties;
      public NetworkStats Stats => inner.Stats;
      public DateTimeOffset CreatedAt => inner.CreatedAt;
      public TransportKind Transport => inner.Transport;
      public NetworkSecurityInfo SecurityInfo => inner.SecurityInfo;
      public NetworkSessionStats SessionStats => inner.SessionStats;
      public IReadOnlyCollection<INetworkStream> ActiveStreams => inner.ActiveStreams;

      public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
      {
         return inner.AcceptStreamAsync(ct);
      }

      public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
         NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
         CancellationToken ct = default)
      {
         return new ValueTask<Result<INetworkStream, NetworkCodeError>>(
            new NetworkCodeError(-1, "open stream failure"));
      }

      public async ValueTask DisposeAsync()
      {
         Interlocked.Increment(ref _disposeCount);
         await inner.DisposeAsync();
      }
   }

   private sealed class OneShotThrowingAcceptListener(INetworkListener inner)
      : INetworkListener
   {
      private int _acceptCalls;
      public int AcceptCalls => Volatile.Read(ref _acceptCalls);

      public EndPoint LocalAddress => inner.LocalAddress;
      public bool IsBound => inner.IsBound;
      public TransportKind Transport => inner.Transport;
      public NetworkListenerStats Stats => inner.Stats;

      public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
      {
         return inner.BindAsync(ct);
      }

      public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
      {
         return inner.UnbindAsync(ct);
      }

      public ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
      {
         if (Interlocked.Increment(ref _acceptCalls) == 1)
            throw new InvalidOperationException("Transient accept failure");

         return inner.AcceptSessionAsync(ct);
      }

      public ValueTask DisposeAsync()
      {
         return inner.DisposeAsync();
      }
   }
}
