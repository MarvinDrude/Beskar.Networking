using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Me.Memory.Results;

namespace Beskar.Networking.Transports.Tcp;

public sealed class TcpNetworkClient(TcpTransportOptions options)
   : INetworkClient, IDisposable
{
   private readonly TcpTransportOptions _options = options;
   private readonly TcpIoQueueRegistry _ioQueueRegistry = new(options);

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint, CancellationToken ct = default)
   {
      Socket? socket = null;
      try
      {
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

         Stream? stream = null;
         if (_options.UseSsl)
         {
            var networkStream = new NetworkStream(socket, ownsSocket: true);
            var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false);

            var sslOptions = _options.SslClientOptions ?? _options.StreamOptions.SslClientOptions;
            if (sslOptions is null)
            {
               socket.Dispose();
               return new NetworkCodeError(-1, "SSL client authentication options are missing.");
            }

            await sslStream.AuthenticateAsClientAsync(sslOptions, ct);
            stream = sslStream;
         }
         else if (_options.ForceStreamBased)
         {
            stream = new NetworkStream(socket, ownsSocket: true);
         }

         var connection = _ioQueueRegistry.Create(socket, stream);

         var localEndPoint = socket.LocalEndPoint ?? socket.RemoteEndPoint ?? endPoint;
         var remoteEndPoint = socket.RemoteEndPoint ?? endPoint;

         var session = new TcpNetworkSession(localEndPoint, remoteEndPoint, connection, _ioQueueRegistry.ReturnAsync);
         return session;
      }
      catch (SocketException ex)
      {
         socket?.Dispose();
         return new NetworkCodeError(ex.ErrorCode, ex.Message);
      }
      catch (Exception ex)
      {
         socket?.Dispose();
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   public void Dispose()
   {
      _ioQueueRegistry.Dispose();
   }
}
