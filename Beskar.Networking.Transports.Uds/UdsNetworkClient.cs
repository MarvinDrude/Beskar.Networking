using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;

namespace Beskar.Networking.Transports.Uds;

public sealed class UdsNetworkClient(UdsTransportOptions options)
   : INetworkClient
{
   public TransportKind Transport => TransportKind.UnixDomainSocket;

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

   private readonly UdsTransportOptions _options = options;
   private readonly UdsIoQueueRegistry _ioQueueRegistry = new(options);

   private UdsNetworkSession? _activeSession;

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint, CancellationToken ct = default)
   {
      Socket? socket = null;
      IDuplexPipe? connection = null;

      try
      {
         TraceLogger.LogClientInfo("UDS ConnectAsync: Initiating UDS socket connection to {0}", endPoint);
         
         if (endPoint is not UnixDomainSocketEndPoint udsEndPoint)
         {
            return new NetworkCodeError(-1, "EndPoint must be a UnixDomainSocketEndPoint.");
         }

         var socketPath = udsEndPoint.ToString();
         if (socketPath.Length > 104)
         {
            throw new ArgumentException($"Unix Domain Socket path '{socketPath}' exceeds the maximum allowed length of 104 characters (path length: {socketPath.Length}).");
         }

         socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);

         if (_options.SendBufferSize.HasValue)
         {
            socket.SendBufferSize = _options.SendBufferSize.Value;
         }
         if (_options.ReceiveBufferSize.HasValue)
         {
            socket.ReceiveBufferSize = _options.ReceiveBufferSize.Value;
         }
         if (_options.LingerState is not null)
         {
            socket.LingerState = _options.LingerState;
         }

         await socket.ConnectAsync(endPoint, ct);
         TraceLogger.LogClientInfo("UDS ConnectAsync: Socket successfully connected to {0}", socket.RemoteEndPoint);

         connection = _ioQueueRegistry.Create(socket);

         var localEndPoint = socket.LocalEndPoint ?? endPoint;
         var remoteEndPoint = socket.RemoteEndPoint ?? endPoint;

         var session = new UdsNetworkSession(localEndPoint, remoteEndPoint, connection, _ioQueueRegistry.ReturnAsync);

         var oldSession = Interlocked.Exchange(ref _activeSession, session);
         if (oldSession is not null)
         {
            await oldSession.DisposeAsync();
         }

         Interlocked.Increment(ref _connectionsEstablished);
         session.SessionClosedToken.Register(() => Interlocked.Increment(ref _connectionsLost));

         TraceLogger.LogClientInfo("UDS ConnectAsync: Network session {0} successfully established", session.Id);
         return session;
      }
      catch (SocketException ex)
      {
         if (connection is not null)
         {
            await _ioQueueRegistry.ReturnAsync(connection);
         }
         else
         {
            socket?.Dispose();
         }

         TraceLogger.LogClientError("UDS ConnectAsync: Socket error connecting to {0}: {1}", endPoint, ex.Message);
         return new NetworkCodeError(ex.ErrorCode, ex.Message);
      }
      catch (Exception ex)
      {
         if (connection is not null)
         {
            await _ioQueueRegistry.ReturnAsync(connection);
         }
         else
         {
            socket?.Dispose();
         }

         TraceLogger.LogClientError("UDS ConnectAsync: Unexpected error connecting to {0}: {1}", endPoint, ex.Message);
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
