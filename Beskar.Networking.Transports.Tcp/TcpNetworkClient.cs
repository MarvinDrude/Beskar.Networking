using System.Diagnostics.CodeAnalysis;
using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using System.Diagnostics;
using Beskar.Networking.Abstractions.Telemetry;

namespace Beskar.Networking.Transports.Tcp;

public sealed class TcpNetworkClient(TcpTransportOptions options)
   : INetworkClient
{
   public TransportKind Transport => TransportKind.Tcp;

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

   private readonly TcpTransportOptions _options = options;
   private readonly TcpIoQueueRegistry _ioQueueRegistry = new(options);

   private TcpNetworkSession? _activeSession;

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint, CancellationToken ct = default)
   {
      Socket? socket = null;
      Stream? stream = null;
      IDuplexPipe? connection = null;

      try
      {
         TraceLogger.LogClientInfo("TCP ConnectAsync: Initiating socket connection to {0}", endPoint);
         socket = new Socket(SocketType.Stream, ProtocolType.Tcp);

         _options.ConfigureSocket(socket);

         await socket.ConnectAsync(endPoint, ct);
         TraceLogger.LogClientInfo("TCP ConnectAsync: Socket successfully connected to {0} (Local: {1})", socket.RemoteEndPoint, socket.LocalEndPoint);

         if (_options.UseSsl)
         {
            TraceLogger.LogClientInfo("TCP ConnectAsync: Starting SSL client authentication for {0}", endPoint);
            var networkStream = new NetworkStream(socket, ownsSocket: true);
            var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false);

            var sslOptions = _options.SslClientOptions ?? _options.StreamOptions.SslClientOptions;
            if (sslOptions is null)
            {
               await sslStream.DisposeAsync();
               TraceLogger.LogClientError("TCP ConnectAsync: SSL authentication failed. SslClientOptions are missing.");
               return new NetworkCodeError(-1, "SSL client authentication options are missing.");
            }

            using var handshakeTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
            handshakeTimeoutCts.CancelAfter(_options.SslHandshakeTimeout);

            var start = Stopwatch.GetTimestamp();
            try
            {
               await sslStream.AuthenticateAsClientAsync(sslOptions, handshakeTimeoutCts.Token);
               TransportMetrics.RecordTlsHandshakeDuration(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            }
            catch (Exception ex)
            {
               TransportMetrics.RecordTlsHandshakeFailure(ex.Message);
               throw;
            }
            stream = sslStream;
            TraceLogger.LogClientInfo("TCP ConnectAsync: SSL client successfully authenticated for {0}", endPoint);
         }
         else if (_options.ForceStreamBased)
         {
            stream = new NetworkStream(socket, ownsSocket: true);
         }

         connection = _ioQueueRegistry.Create(socket, stream);

         var localEndPoint = socket.LocalEndPoint ?? socket.RemoteEndPoint ?? endPoint;
         var remoteEndPoint = socket.RemoteEndPoint ?? endPoint;

         var session = new TcpNetworkSession(localEndPoint, remoteEndPoint, connection, _ioQueueRegistry.ReturnAsync);

         var oldSession = Interlocked.Exchange(ref _activeSession, session);
         if (oldSession is not null)
         {
            await oldSession.DisposeAsync();
         }

         Interlocked.Increment(ref _connectionsEstablished);
         session.SessionClosedToken.Register(() => Interlocked.Increment(ref _connectionsLost));

         TraceLogger.LogClientInfo("TCP ConnectAsync: Network session {0} successfully established for {1}", session.Id, remoteEndPoint);
         return session;
      }
      catch (SocketException ex)
      {
         TransportMetrics.RecordConnectionFailed(TransportKind.Tcp, ex.SocketErrorCode.ToString());
         if (connection is not null)
         {
            await _ioQueueRegistry.ReturnAsync(connection);
         }
         else if (stream is not null)
         {
            await stream.DisposeAsync();
         }
         else
         {
            socket?.Dispose();
         }

         TraceLogger.LogClientError("TCP ConnectAsync: Socket error connecting to {0}: {1}", endPoint, ex.Message);
         return new NetworkCodeError(ex.ErrorCode, ex.Message);
      }
      catch (Exception ex)
      {
         TransportMetrics.RecordConnectionFailed(TransportKind.Tcp, ex.GetType().Name);
         if (connection is not null)
         {
            await _ioQueueRegistry.ReturnAsync(connection);
         }
         else if (stream is not null)
         {
            await stream.DisposeAsync();
         }
         else
         {
            socket?.Dispose();
         }

         TraceLogger.LogClientError("TCP ConnectAsync: Unexpected error connecting to {0}: {1}", endPoint, ex.Message);
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
