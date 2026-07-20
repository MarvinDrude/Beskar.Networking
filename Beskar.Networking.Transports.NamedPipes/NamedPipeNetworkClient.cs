using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.IO.Pipes;
using System.Net;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;

namespace Beskar.Networking.Transports.NamedPipes;

public sealed class NamedPipeNetworkClient(NamedPipeTransportOptions options)
   : INetworkClient
{
   public TransportKind Transport => TransportKind.NamedPipe;

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

   private readonly NamedPipeTransportOptions _options = options;
   private readonly NamedPipeIoQueueRegistry _ioQueueRegistry = new(options);

   private NamedPipeNetworkSession? _activeSession;

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint, CancellationToken ct = default)
   {
      NamedPipeClientStream? pipeStream = null;
      IDuplexPipe? connection = null;

      try
      {
         TraceLogger.LogClientInfo("NamedPipe ConnectAsync: Initiating connection to {0}", endPoint);

         if (endPoint is not NamedPipeEndPoint namedPipeEndPoint)
         {
            return new NetworkCodeError(-1, "EndPoint must be a NamedPipeEndPoint.");
         }

         pipeStream = new NamedPipeClientStream(
            namedPipeEndPoint.ServerName,
            namedPipeEndPoint.PipeName,
            System.IO.Pipes.PipeDirection.InOut,
            System.IO.Pipes.PipeOptions.Asynchronous
         );

         await pipeStream.ConnectAsync(ct);
         TraceLogger.LogClientInfo("NamedPipe ConnectAsync: Successfully connected to {0}", endPoint);

         connection = _ioQueueRegistry.Create(pipeStream);

         var session = new NamedPipeNetworkSession(namedPipeEndPoint, namedPipeEndPoint, connection, _ioQueueRegistry.ReturnAsync);

         var oldSession = Interlocked.Exchange(ref _activeSession, session);
         if (oldSession is not null)
         {
            await oldSession.DisposeAsync();
         }

         Interlocked.Increment(ref _connectionsEstablished);
         session.SessionClosedToken.Register(() => Interlocked.Increment(ref _connectionsLost));

         TraceLogger.LogClientInfo("NamedPipe ConnectAsync: Network session {0} successfully established", session.Id);
         return session;
      }
      catch (Exception ex)
      {
         if (connection is not null)
         {
            await _ioQueueRegistry.ReturnAsync(connection);
         }
         else
         {
            if (pipeStream is not null)
            {
               await pipeStream.DisposeAsync();
            }
         }

         TraceLogger.LogClientError("NamedPipe ConnectAsync: Error connecting to {0}: {1}", endPoint, ex.Message);
         return new NetworkCodeError(-1, ex.Message);
      }
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
      var session = Interlocked.Exchange(ref _activeSession, null);
      if (session is not null)
      {
         try
         {
            await session.DisposeAsync();
         }
         catch
         {
            // Ignored
         }
      }

      await _ioQueueRegistry.DisposeAsync();
   }
}
