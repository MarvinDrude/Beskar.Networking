using System.Net;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Tcp;
using Me.Memory.Results;

namespace Beskar.Networking.Transports.Ws;

/// <summary>
/// A high-performance WebSocket client.
/// </summary>
public sealed class WsNetworkClient : INetworkClient, IDisposable
{
   private readonly WsTransportOptions _options;
   private readonly TcpNetworkClient _tcpClient;

   public WsNetworkClient(WsTransportOptions options)
   {
      _options = options;
      _tcpClient = new TcpNetworkClient(options.TcpOptions);
   }

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint,
      CancellationToken ct = default)
   {
      var connectResult = await _tcpClient.ConnectAsync(endPoint, ct);
      if (connectResult.Failed)
      {
         return connectResult.Error;
      }

      var tcpSession = connectResult.Success;
      try
      {
         var tcpStreamResult = await tcpSession.AcceptStreamAsync(ct);
         if (tcpStreamResult.Failed)
         {
            await ((IAsyncDisposable)tcpSession).DisposeAsync();
            return tcpStreamResult.Error;
         }

         var tcpPipe = tcpStreamResult.Success.Transport;
         var handshakeSuccess = await WsHandshake.ClientHandshakeAsync(tcpPipe, endPoint, _options, ct);
         if (!handshakeSuccess)
         {
            await ((IAsyncDisposable)tcpSession).DisposeAsync();
            return new NetworkCodeError(-1, "WebSocket handshake verification failed.");
         }

         // Clients must mask outgoing frames according to RFC 6455
         var wsPipe = new WsDuplexPipe(tcpPipe, maskOutgoing: true);
         var wsSession = new WsNetworkSession(tcpSession, wsPipe);

         return wsSession;
      }
      catch (Exception ex)
      {
         await ((IAsyncDisposable)tcpSession).DisposeAsync();
         return new NetworkCodeError(-1, $"Handshake failed: {ex.Message}");
      }
   }

   public void Dispose()
   {
      _tcpClient.Dispose();
   }
}
