using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Me.Memory.Results;

namespace Beskar.Networking.Transports.Tcp;

public sealed class TcpNetworkListener(
   EndPoint localAddress,
   TcpTransportOptions options)
   : INetworkListener
{
   public EndPoint LocalAddress { get; } = localAddress;

   private readonly TcpTransportOptions _options = options;
   private readonly TcpIoQueueRegistry _ioQueueRegistry = new(options);

   private Socket? _listenerSocket;

   public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
   {
      try
      {
         var socket = new Socket(LocalAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

         socket.Bind(LocalAddress);
         socket.Listen(backlog: 512);

         _listenerSocket = socket;
         return ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      }
      catch (SocketException ex)
      {
         return new ValueTask<VoidResult<NetworkCodeError>>(
            new NetworkCodeError(ex.ErrorCode, ex.Message));
      }
      catch (Exception ex)
      {
         return new ValueTask<VoidResult<NetworkCodeError>>(
            new NetworkCodeError(-1, ex.Message));
      }
   }

   public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      try
      {
         var socket = Interlocked.Exchange(ref _listenerSocket, null);
         socket?.Close();

         return ValueTask.FromResult<VoidResult<NetworkCodeError>>(true);
      }
      catch (Exception ex)
      {
         return new ValueTask<VoidResult<NetworkCodeError>>(
            new NetworkCodeError(-1, ex.Message));
      }
   }

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
   {
      var socket = _listenerSocket;
      if (socket is null)
      {
         return new NetworkCodeError(-1, "Listener is not bound. Call BindAsync first.");
      }

      try
      {
         var clientSocket = await socket.AcceptAsync(ct);
         Stream? stream = null;

         if (_options.IsStreamBased)
         {
            var streamResult = await CreateSessionStream(clientSocket, ct);
            if (streamResult.Failed) return streamResult.Error;

            stream = streamResult.Success;
         }

         var duplex = _ioQueueRegistry.Create(clientSocket, stream);
         var session = new TcpNetworkSession()
         {
            DuplexPipe = duplex,
            LocalAddress = clientSocket.LocalEndPoint
               ?? throw new InvalidOperationException("Local endpoint cannot be null after accept"),
            RemoteAddress = clientSocket.RemoteEndPoint
               ?? throw new InvalidOperationException("Remote endpoint cannot be null after accept"),
         };

         return session;
      }
      catch (SocketException ex)
      {
         return new NetworkCodeError(ex.ErrorCode, ex.Message);
      }
      catch (Exception ex)
      {
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   private async ValueTask<Result<Stream, NetworkCodeError>> CreateSessionStream(Socket socket,
      CancellationToken ct = default)
   {
      Stream stream;
      var networkStream = new NetworkStream(socket, ownsSocket: true);

      if (_options.UseSsl)
      {
         var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false);

         var options = _options.SslServerOptions ?? _options.StreamOptions.SslServerOptions;
         if (options is null)
         {
            return new NetworkCodeError(-1, "SSL options are not set. Either use TcpTransportOptions.SslServerOptions or TcpTransportOptions.StreamOptions.SslServerOptions.");
         }

         await sslStream.AuthenticateAsServerAsync(options, ct);
         stream = sslStream;
      }
      else
      {
         stream = networkStream;
      }

      return stream;
   }
}
