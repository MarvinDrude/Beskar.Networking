using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading.Channels;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Me.Memory.Results;

namespace Beskar.Networking.Transports.Tcp;

/// <summary>
/// A high-performance TCP transport listener that decouples accepted connections
/// from SSL/TLS handshakes using a non-blocking background queue.
/// </summary>
public sealed class TcpNetworkListener(
   EndPoint localAddress,
   TcpTransportOptions options)
   : INetworkListener
{
   public EndPoint LocalAddress { get; } = localAddress;

   private readonly TcpTransportOptions _options = options;
   private readonly TcpIoQueueRegistry _ioQueueRegistry = new(options);

   private Socket? _listenerSocket;
   private CancellationTokenSource? _acceptCts;

   private readonly Channel<Result<INetworkSession, NetworkCodeError>> _sessionChannel =
      Channel.CreateUnbounded<Result<INetworkSession, NetworkCodeError>>(new UnboundedChannelOptions
      {
         SingleWriter = false,
         SingleReader = true
      });

   public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
   {
      try
      {
         var socket = new Socket(LocalAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);
         socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
         socket.Bind(LocalAddress);
         socket.Listen(512);

         _listenerSocket = socket;
         _acceptCts = new CancellationTokenSource();

         _ = AcceptLoopAsync(socket, _acceptCts.Token);
         return new ValueTask<VoidResult<NetworkCodeError>>(true);
      }
      catch (SocketException ex)
      {
         return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(ex.ErrorCode, ex.Message));
      }
      catch (Exception ex)
      {
         return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(-1, ex.Message));
      }
   }

   public ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      try
      {
         _acceptCts?.Cancel();
         _acceptCts?.Dispose();

         _acceptCts = null;

         var socket = Interlocked.Exchange(ref _listenerSocket, null);
         socket?.Close();

         _sessionChannel.Writer.TryComplete();

         return new ValueTask<VoidResult<NetworkCodeError>>(true);
      }
      catch (Exception ex)
      {
         return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(-1, ex.Message));
      }
   }

   public ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
   {
      if (_listenerSocket is null)
      {
         return ValueTask.FromResult<Result<INetworkSession, NetworkCodeError>>(
            new NetworkCodeError(-1, "Listener is not bound. Call BindAsync first."));
      }

      try
      {
         return _sessionChannel.Reader.TryRead(out var result)
            ? ValueTask.FromResult(result)
            : Awaited();
      }
      catch (ChannelClosedException)
      {
         return ValueTask.FromResult<Result<INetworkSession, NetworkCodeError>>(
            new NetworkCodeError(-1, "Listener has been unbound and session channel is closed."));
      }

      async ValueTask<Result<INetworkSession, NetworkCodeError>> Awaited()
      {
         return await _sessionChannel.Reader.ReadAsync(ct);
      }
   }

   private async Task AcceptLoopAsync(Socket listenerSocket, CancellationToken token)
   {
      while (!token.IsCancellationRequested)
      {
         try
         {
            var clientSocket = await listenerSocket.AcceptAsync(token);

            var localEndPoint = clientSocket.LocalEndPoint;
            if (localEndPoint is null)
            {
               WriteToSessionChannel(new NetworkCodeError(-1, "Failed to get local endpoint."));
               return;
            }

            var remoteEndPoint = clientSocket.RemoteEndPoint;
            if (remoteEndPoint is null)
            {
               WriteToSessionChannel(new NetworkCodeError(-1, "Failed to get remote endpoint."));
               return;
            }

            _ = Task.Run(() => HandshakeAndEnqueueAsync(clientSocket, localEndPoint, remoteEndPoint, token), token);
         }
         catch (OperationCanceledException)
         {
            break;
         }
         catch (SocketException ex)
         {
            if (token.IsCancellationRequested || _listenerSocket is null)
            {
               break;
            }

            WriteToSessionChannel(new NetworkCodeError(ex.ErrorCode, $"Listener acceptance error: {ex.Message}"));
         }
         catch (Exception ex)
         {
            if (token.IsCancellationRequested || _listenerSocket is null)
            {
               break;
            }

            WriteToSessionChannel(new NetworkCodeError(-1, $"Listener acceptance error: {ex.Message}"));
         }
      }
   }

   private async Task HandshakeAndEnqueueAsync(
      Socket socket,
      EndPoint localEndPoint,
      EndPoint remoteEndPoint,
      CancellationToken token)
   {
      try
      {
         Stream? stream = null;
         if (_options.UseSsl)
         {
            using var handshakeTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            handshakeTimeoutCts.CancelAfter(TimeSpan.FromSeconds(10));

            var networkStream = new NetworkStream(socket, ownsSocket: true);
            var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false);

            var sslOptions = _options.SslServerOptions ?? _options.StreamOptions.SslServerOptions;
            if (sslOptions is null)
            {
               WriteToSessionChannel(new NetworkCodeError(-1, "SSL server authentication options are missing."));
               return;
            }

            await sslStream.AuthenticateAsServerAsync(sslOptions, handshakeTimeoutCts.Token);
            stream = sslStream;
         }
         else if (_options.ForceStreamBased)
         {
            stream = new NetworkStream(socket, ownsSocket: true);
         }

         var connection = _ioQueueRegistry.Create(socket, stream);
         var session = new TcpNetworkSession(localEndPoint, remoteEndPoint, connection);

         await _sessionChannel.Writer.WriteAsync(session, token);
      }
      catch (OperationCanceledException)
      {
         // ignored
      }
      catch (SocketException ex)
      {
         WriteToSessionChannel(new NetworkCodeError(ex.ErrorCode, ex.Message));
         socket.Dispose();
      }
      catch (Exception ex)
      {
         WriteToSessionChannel(new NetworkCodeError(-1, ex.Message));
         socket.Dispose();
      }
   }

   private void WriteToSessionChannel(Result<INetworkSession, NetworkCodeError> result)
   {
      _sessionChannel.Writer.TryWrite(result);
   }
}
