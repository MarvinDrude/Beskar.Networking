using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.Tcp;

public sealed class TcpNetworkClient(TcpTransportOptions options)
   : INetworkClient
{
   private readonly TcpTransportOptions _options = options;
   private readonly TcpIoQueueRegistry _ioQueueRegistry = new(options);

   private TcpNetworkSession? _activeSession;

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint, CancellationToken ct = default)
   {
      Socket? socket = null;
      try
      {
         TraceLogger.LogClientInfo("TCP ConnectAsync: Initiating socket connection to {0}", endPoint);
         socket = new Socket(endPoint.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

         if (_options.NoDelay)
         {
            socket.NoDelay = true;
         }
         if (_options.SendBufferSize.HasValue)
         {
            socket.SendBufferSize = _options.SendBufferSize.Value;
         }
         if (_options.ReceiveBufferSize.HasValue)
         {
            socket.ReceiveBufferSize = _options.ReceiveBufferSize.Value;
         }

         await socket.ConnectAsync(endPoint, ct);
         TraceLogger.LogClientInfo("TCP ConnectAsync: Socket successfully connected to {0} (Local: {1})", socket.RemoteEndPoint, socket.LocalEndPoint);

         Stream? stream = null;
         if (_options.UseSsl)
         {
            TraceLogger.LogClientInfo("TCP ConnectAsync: Starting SSL client authentication for {0}", endPoint);
            var networkStream = new NetworkStream(socket, ownsSocket: true);
            var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false);

            var sslOptions = _options.SslClientOptions ?? _options.StreamOptions.SslClientOptions;
            if (sslOptions is null)
            {
               socket.Dispose();
               TraceLogger.LogClientError("TCP ConnectAsync: SSL authentication failed. SslClientOptions are missing.");
               return new NetworkCodeError(-1, "SSL client authentication options are missing.");
            }

            await sslStream.AuthenticateAsClientAsync(sslOptions, ct);
            stream = sslStream;
            TraceLogger.LogClientInfo("TCP ConnectAsync: SSL client successfully authenticated for {0}", endPoint);
         }
         else if (_options.ForceStreamBased)
         {
            stream = new NetworkStream(socket, ownsSocket: true);
         }

         var connection = _ioQueueRegistry.Create(socket, stream);

         var localEndPoint = socket.LocalEndPoint ?? socket.RemoteEndPoint ?? endPoint;
         var remoteEndPoint = socket.RemoteEndPoint ?? endPoint;

         var session = new TcpNetworkSession(localEndPoint, remoteEndPoint, connection, _ioQueueRegistry.ReturnAsync);

         var oldSession = Interlocked.Exchange(ref _activeSession, session);
         if (oldSession is not null)
         {
            await oldSession.DisposeAsync();
         }

         TraceLogger.LogClientInfo("TCP ConnectAsync: Network session {0} successfully established for {1}", session.Id, remoteEndPoint);
         return session;
      }
      catch (SocketException ex)
      {
         socket?.Dispose();
         TraceLogger.LogClientError("TCP ConnectAsync: Socket error connecting to {0}: {1}", endPoint, ex.Message);
         return new NetworkCodeError(ex.ErrorCode, ex.Message);
      }
      catch (Exception ex)
      {
         socket?.Dispose();
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

      _ioQueueRegistry.Dispose();
   }
}
