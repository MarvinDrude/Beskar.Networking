using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Net;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.Memory;

/// <summary>
/// An in-memory implementation of <see cref="INetworkClient"/>.
/// </summary>
public sealed class MemoryNetworkClient(MemoryTransportOptions options)
   : INetworkClient
{
   public TransportKind Transport => TransportKind.Memory;

   [MemberNotNullWhen(true, nameof(_activeSession), nameof(Session))]
   public bool IsConnected => _activeSession is not null
      && !_activeSession.SessionClosedToken.IsCancellationRequested;

   public INetworkSession? Session => _activeSession;

   public EndPoint? LocalAddress => _activeSession?.LocalAddress;
   public EndPoint? RemoteAddress => _activeSession?.RemoteAddress;

   private long _connectionsEstablished;
   private long _connectionsLost;

   public NetworkClientStats Stats => new()
   {
      ConnectionsEstablished = Interlocked.Read(ref _connectionsEstablished),
      ConnectionsLost = Interlocked.Read(ref _connectionsLost)
   };

   private readonly MemoryTransportOptions _options = options;
   private MemoryNetworkSession? _activeSession;

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint, CancellationToken ct = default)
   {
      if (endPoint is not MemoryEndPoint memoryEndPoint)
      {
         return new NetworkCodeError(-1, "EndPoint must be a MemoryEndPoint.");
      }

      TraceLogger.LogClientInfo("Memory ConnectAsync: Looking up listener for {0}", memoryEndPoint);
      var listener = MemoryTransportRegistry.GetListener(memoryEndPoint.Address);
      if (listener is null || !listener.IsBound)
      {
         TraceLogger.LogClientError("Memory ConnectAsync: Connection refused. No listener bound to {0}", memoryEndPoint);
         return new NetworkCodeError(-1, $"Connection refused. No listener bound to '{memoryEndPoint.Address}'.");
      }

      // Create connected pipelines
      var clientToServer = new Pipe();
      var serverToClient = new Pipe();

      var clientConnection = new DuplexPipe(serverToClient.Reader, clientToServer.Writer);
      var serverConnection = new DuplexPipe(clientToServer.Reader, serverToClient.Writer);

      var clientLocalAddress = new MemoryEndPoint($"client-{Guid.NewGuid():N}");

      var clientSession = new MemoryNetworkSession(clientLocalAddress, memoryEndPoint, clientConnection);
      var serverSession = new MemoryNetworkSession(memoryEndPoint, clientLocalAddress, serverConnection);

      clientSession.SetPeer(serverSession);
      serverSession.SetPeer(clientSession);

      TraceLogger.LogClientInfo("Memory ConnectAsync: Enqueuing session to listener for {0}", memoryEndPoint);
      var enqueued = await listener.TryEnqueueSessionAsync(serverSession, ct);
      if (!enqueued)
      {
         await clientSession.DisposeAsync();
         await serverSession.DisposeAsync();

         TraceLogger.LogClientError("Memory ConnectAsync: Failed to enqueue session on listener.");
         return new NetworkCodeError(-1, "Failed to enqueue session on listener.");
      }

      var oldSession = Interlocked.Exchange(ref _activeSession, clientSession);
      if (oldSession is not null)
      {
         await oldSession.DisposeAsync();
      }

      Interlocked.Increment(ref _connectionsEstablished);
      clientSession.SessionClosedToken.Register(() => Interlocked.Increment(ref _connectionsLost));

      TraceLogger.LogClientInfo("Memory ConnectAsync: Session successfully established for {0}", memoryEndPoint);
      return clientSession;
   }

   public async ValueTask DisconnectAsync(CancellationToken ct = default)
   {
      var session = Interlocked.Exchange(ref _activeSession, null);
      if (session is not null)
      {
         await session.DisposeAsync();
      }
   }

   public async ValueTask DisposeAsync()
   {
      await DisconnectAsync();
   }

   private sealed class DuplexPipe(PipeReader reader, PipeWriter writer) : IDuplexPipe
   {
      public PipeReader Input => reader;
      public PipeWriter Output => writer;
   }
}
