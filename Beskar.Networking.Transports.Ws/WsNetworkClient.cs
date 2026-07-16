using System.Net;
using System.Threading;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Transports.Tcp;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;

namespace Beskar.Networking.Transports.Ws;

/// <summary>
/// A high-performance WebSocket client.
/// </summary>
public sealed class WsNetworkClient(WsTransportOptions options) : INetworkClient
{
   public TransportKind Transport => TransportKind.WebSocket;

   public bool IsConnected => _activeSession is not null
      && !_activeSession.SessionClosedToken.IsCancellationRequested;

   private long _connectionsEstablished;
   private long _connectionsLost;

   public NetworkClientStats Stats => new()
   {
      ConnectionsEstablished = Interlocked.Read(ref _connectionsEstablished),
      ConnectionsLost = Interlocked.Read(ref _connectionsLost)
   };

   private readonly WsTransportOptions _options = options;
   private readonly TcpNetworkClient _tcpClient = new(options.TcpOptions);

   private WsNetworkSession? _activeSession;

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint,
      CancellationToken ct = default)
   {
      TraceLogger.LogClientInfo("WS ConnectAsync: Initiating WebSocket connection over TCP to {0} (Path: {1})", endPoint, _options.Path);

      var connectResult = await _tcpClient.ConnectAsync(endPoint, ct);
      if (connectResult.Failed)
      {
         TraceLogger.LogClientError("WS ConnectAsync: Failed to establish TCP connection to {0}: {1}", endPoint, connectResult.Error.Message);
         return connectResult.Error;
      }

      var tcpSession = connectResult.Success;
      WsDuplexPipe? wsPipe = null;

      try
      {
         var tcpStreamResult = await tcpSession.AcceptStreamAsync(ct);
         if (tcpStreamResult.Failed)
         {
            TraceLogger.LogClientError("WS ConnectAsync: Failed to accept TCP stream for session {0}: {1}", tcpSession.Id, tcpStreamResult.Error.Message);
            await tcpSession.DisposeAsync();

            return tcpStreamResult.Error;
         }

         var tcpPipe = tcpStreamResult.Success.Transport;
         var handshakeSuccess = await WsHandshake.ClientHandshakeAsync(tcpPipe, endPoint, _options, ct);

         if (!handshakeSuccess)
         {
            TraceLogger.LogClientError("WS ConnectAsync: WebSocket handshake verification failed for session {0}.", tcpSession.Id);
            await tcpSession.DisposeAsync();
            return new NetworkCodeError(-1, "WebSocket handshake verification failed.");
         }

         wsPipe = new WsDuplexPipe(tcpPipe, maskOutgoing: true);
         var wsSession = new WsNetworkSession(tcpSession, wsPipe);

         var oldSession = Interlocked.Exchange(ref _activeSession, wsSession);
         if (oldSession is not null)
         {
            await oldSession.DisposeAsync();
         }

         Interlocked.Increment(ref _connectionsEstablished);
         wsSession.SessionClosedToken.Register(() => Interlocked.Increment(ref _connectionsLost));

         TraceLogger.LogClientInfo("WS ConnectAsync: WebSocket session {0} successfully established for {1}", wsSession.Id, endPoint);
         return wsSession;
      }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("WS ConnectAsync: Unexpected error establishing WebSocket connection to {0}: {1}", endPoint, ex.Message);
         if (wsPipe is not null)
         {
            await wsPipe.DisposeAsync();
         }

         await tcpSession.DisposeAsync();
         return new NetworkCodeError(-1, $"Handshake failed: {ex.Message}");
      }
   }

   public async ValueTask DisconnectAsync(CancellationToken ct = default)
   {
      var session = Interlocked.Exchange(ref _activeSession, null);
      if (session is not null)
      {
         await session.DisposeAsync();
      }

      await _tcpClient.DisconnectAsync(ct);
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

      await _tcpClient.DisposeAsync();
   }
}
