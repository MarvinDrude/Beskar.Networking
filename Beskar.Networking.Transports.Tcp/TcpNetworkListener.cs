using System.IO.Pipelines;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Threading.Channels;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using System.Diagnostics;
using Beskar.Networking.Abstractions.Telemetry;

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
   private readonly EndPoint _configuredLocalAddress = localAddress;
   public EndPoint LocalAddress => _listenerSocket?.LocalEndPoint ?? _configuredLocalAddress;

   public TransportKind Transport => TransportKind.Tcp;
   public bool IsBound => _listenerSocket is not null;

   private long _binds;
   private long _unbinds;
   private long _sessionsAccepted;

   public NetworkListenerStats Stats => new()
   {
      Binds = Interlocked.Read(ref _binds),
      Unbinds = Interlocked.Read(ref _unbinds),
      SessionsAccepted = Interlocked.Read(ref _sessionsAccepted)
   };

   private readonly TcpTransportOptions _options = options;
   private readonly TcpIoQueueRegistry _ioQueueRegistry = new(options);

   private Socket? _listenerSocket;
   private CancellationTokenSource? _acceptCts;
   private SemaphoreSlim? _handshakeSemaphore;

   private int _disposedState; // 0 = active, 1 = disposed

   private Channel<Result<INetworkSession, NetworkCodeError>> _sessionChannel =
      Channel.CreateBounded<Result<INetworkSession, NetworkCodeError>>(new BoundedChannelOptions(1024)
      {
         SingleWriter = false,
         SingleReader = true,
         FullMode = BoundedChannelFullMode.Wait
      });

   public ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
   {
      try
      {
         _sessionChannel = Channel.CreateBounded<Result<INetworkSession, NetworkCodeError>>(
            new BoundedChannelOptions(_options.MaxPendingConnections)
            {
               SingleWriter = false,
               SingleReader = true,
               FullMode = BoundedChannelFullMode.Wait
            });

         TraceLogger.LogServerInfo("TCP Listener: Binding socket to address {0}", LocalAddress);
         var socket = new Socket(LocalAddress.AddressFamily, SocketType.Stream, ProtocolType.Tcp);

         socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
         socket.Bind(LocalAddress);
         socket.Listen(_options.Backlog);

         _listenerSocket = socket;
         _acceptCts = new CancellationTokenSource();
         _handshakeSemaphore = new SemaphoreSlim(_options.MaxConcurrentHandshakes);

         _ = AcceptLoopAsync(socket, _acceptCts.Token);
         TraceLogger.LogServerInfo("TCP Listener: Successfully bound and listening on {0}", LocalAddress);

         Interlocked.Increment(ref _binds);
         TransportMetrics.RecordListenerStarted(TransportKind.Tcp);

         return new ValueTask<VoidResult<NetworkCodeError>>(true);
      }
      catch (SocketException ex)
      {
         TraceLogger.LogServerError("TCP Listener: Failed to bind to {0}: {1}", LocalAddress, ex.Message);
         return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(ex.ErrorCode, ex.Message));
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("TCP Listener: Failed to bind to {0}: {1}", LocalAddress, ex.Message);
         return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(-1, ex.Message));
      }
   }

   public async ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      try
      {
         TraceLogger.LogServerInfo("TCP Listener: Unbinding and stopping listener socket on {0}", LocalAddress);
         if (_acceptCts is not null)
         {
            await _acceptCts.CancelAsync();

            _acceptCts.Dispose();
            _acceptCts = null;
         }

         var socket = Interlocked.Exchange(ref _listenerSocket, null);
         socket?.Close();
         socket?.Dispose();

         var semaphore = Interlocked.Exchange(ref _handshakeSemaphore, null);
         semaphore?.Dispose();

         _sessionChannel.Writer.TryComplete();
         while (_sessionChannel.Reader.TryRead(out var result))
         {
            if (!result.Failed)
            {
               await result.Success.DisposeAsync();
            }
         }

         TraceLogger.LogServerInfo("TCP Listener: Successfully unbound from {0}", LocalAddress);
         Interlocked.Increment(ref _unbinds);
         TransportMetrics.RecordListenerStopped(TransportKind.Tcp);

         return true;
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("TCP Listener: Error during unbind from {0}: {1}", LocalAddress, ex.Message);
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
   {
      if (_listenerSocket is null)
      {
         return new NetworkCodeError(-1, "Listener is not bound. Call BindAsync first.");
      }

      try
      {
         return _sessionChannel.Reader.TryRead(out var result)
            ? result
            : await _sessionChannel.Reader.ReadAsync(ct);
      }
      catch (ChannelClosedException)
      {
         return new NetworkCodeError(-1, "Listener has been unbound and session channel is closed.");
      }
   }

   private async Task AcceptLoopAsync(Socket listenerSocket, CancellationToken token)
   {
      while (!token.IsCancellationRequested)
      {
         var semaphore = _handshakeSemaphore;
         if (semaphore is null)
         {
            break;
         }

         try
         {
            await semaphore.WaitAsync(token);

            Socket clientSocket;
            try
            {
               clientSocket = await listenerSocket.AcceptAsync(token);
            }
            catch
            {
               semaphore.Release();
               throw;
            }

            try
            {
               _options.ConfigureSocket(clientSocket);

               var localEndPoint = clientSocket.LocalEndPoint;
               if (localEndPoint is null)
               {
                  TraceLogger.LogServerError("TCP Listener: Rejected connection. Failed to get local endpoint.");
                  WriteToSessionChannel(new NetworkCodeError(-1, "Failed to get local endpoint."));

                  clientSocket.Dispose();
                  semaphore.Release();
                  continue;
               }

               var remoteEndPoint = clientSocket.RemoteEndPoint;
               if (remoteEndPoint is null)
               {
                  TraceLogger.LogServerError("TCP Listener: Rejected connection. Failed to get remote endpoint.");
                  WriteToSessionChannel(new NetworkCodeError(-1, "Failed to get remote endpoint."));
                  TransportMetrics.RecordConnectionFailed(TransportKind.Tcp, "NoRemoteEndPoint");

                  clientSocket.Dispose();
                  semaphore.Release();
                  continue;
               }

               TraceLogger.LogServerInfo("TCP Listener: Accepted connection from client {0}", remoteEndPoint);
               _ = Task.Run(async () =>
               {
                  try
                  {
                     // ReSharper disable once AccessToDisposedClosure - only runs if no exception happened
                     await HandshakeAndEnqueueAsync(clientSocket, localEndPoint, remoteEndPoint, token);
                  }
                  finally
                  {
                     semaphore.Release();
                  }
               }, token);
            }
            catch (Exception ex)
            {
               TraceLogger.LogServerError("TCP Listener: Error configuring accepted socket: {0}", ex.Message);

               clientSocket.Dispose();
               semaphore.Release();

               throw;
            }
         }
         catch (OperationCanceledException)
         {
            break;
         }
         catch (ObjectDisposedException)
         {
            break;
         }
         catch (SocketException ex)
         {
            if (token.IsCancellationRequested || _listenerSocket is null)
            {
               break;
            }

            TransportMetrics.RecordConnectionFailed(TransportKind.Tcp, ex.SocketErrorCode.ToString());
            TraceLogger.LogServerError("TCP Listener: Socket error accepting client: {0}", ex.Message);
            WriteToSessionChannel(new NetworkCodeError(ex.ErrorCode, $"Listener acceptance error: {ex.Message}"));

            try { await Task.Delay(_options.AcceptExceptionDelay, token); } catch (OperationCanceledException) { break; }
         }
         catch (Exception ex)
         {
            if (token.IsCancellationRequested || _listenerSocket is null)
            {
               break;
            }

            TransportMetrics.RecordConnectionFailed(TransportKind.Tcp, ex.GetType().Name);
            TraceLogger.LogServerError("TCP Listener: Unexpected error accepting client: {0}", ex.Message);
            WriteToSessionChannel(new NetworkCodeError(-1, $"Listener acceptance error: {ex.Message}"));

            try { await Task.Delay(_options.AcceptExceptionDelay, token); } catch (OperationCanceledException) { break; }
         }
      }
   }

   private async Task HandshakeAndEnqueueAsync(
      Socket socket,
      EndPoint localEndPoint,
      EndPoint remoteEndPoint,
      CancellationToken token)
   {
      Stream? stream = null;
      IDuplexPipe? connection = null;
      var success = false;

      try
      {
         if (_options.UseSsl)
         {
            TraceLogger.LogServerInfo("TCP Listener: Beginning SSL server authentication for client {0}", remoteEndPoint);
            using var handshakeTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
            handshakeTimeoutCts.CancelAfter(_options.SslHandshakeTimeout);

            var networkStream = new NetworkStream(socket, ownsSocket: true);
            var sslStream = new SslStream(networkStream, leaveInnerStreamOpen: false);

            var sslOptions = _options.SslServerOptions ?? _options.StreamOptions.SslServerOptions;
            if (sslOptions is null)
            {
               TraceLogger.LogServerError("TCP Listener: SSL handshake aborted for client {0}. SslServerOptions are missing.", remoteEndPoint);
               WriteToSessionChannel(new NetworkCodeError(-1, "SSL server authentication options are missing."));
               await sslStream.DisposeAsync();

               return;
            }

            if (_options.ClientCertificateRequired.HasValue)
            {
               sslOptions.ClientCertificateRequired = _options.ClientCertificateRequired.Value;
            }
            if (_options.ClientCertificateValidationCallback is not null)
            {
               sslOptions.RemoteCertificateValidationCallback = _options.ClientCertificateValidationCallback;
            }
            if (_options.ClientCertificateRevocationMode.HasValue)
            {
               sslOptions.CertificateRevocationCheckMode = _options.ClientCertificateRevocationMode.Value;
            }

            var start = Stopwatch.GetTimestamp();
            try
            {
               await sslStream.AuthenticateAsServerAsync(sslOptions, handshakeTimeoutCts.Token);
               TransportMetrics.RecordTlsHandshakeDuration(Stopwatch.GetElapsedTime(start).TotalMilliseconds);
            }
            catch (Exception ex)
            {
               TransportMetrics.RecordTlsHandshakeFailure(ex.Message);
               throw;
            }
            stream = sslStream;

            TraceLogger.LogServerInfo("TCP Listener: SSL server successfully authenticated client {0}", remoteEndPoint);
         }
         else if (_options.ForceStreamBased)
         {
            stream = new NetworkStream(socket, ownsSocket: true);
         }

         connection = _ioQueueRegistry.Create(socket, stream);
         var session = new TcpNetworkSession(localEndPoint, remoteEndPoint, connection, _ioQueueRegistry.ReturnAsync);

         TraceLogger.LogServerInfo("TCP Listener: Enqueuing network session {0} for client {1}", session.Id, remoteEndPoint);
         Interlocked.Increment(ref _sessionsAccepted);

         await _sessionChannel.Writer.WriteAsync(session, token);
         success = true;
      }
      catch (OperationCanceledException)
      {
         TraceLogger.LogServerError("TCP Listener: Connection handshake timed out or cancelled for client {0}", remoteEndPoint);
      }
      catch (SocketException ex)
      {
         TraceLogger.LogServerError("TCP Listener: Socket error during handshake for client {0}: {1}", remoteEndPoint, ex.Message);
         WriteToSessionChannel(new NetworkCodeError(ex.ErrorCode, ex.Message));
         TransportMetrics.RecordConnectionFailed(TransportKind.Tcp, ex.SocketErrorCode.ToString());
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("TCP Listener: Unexpected error during handshake for client {0}: {1}", remoteEndPoint, ex.Message);
         WriteToSessionChannel(new NetworkCodeError(-1, ex.Message));
         TransportMetrics.RecordConnectionFailed(TransportKind.Tcp, ex.GetType().Name);
      }
      finally
      {
         if (!success)
         {
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
               socket.Dispose();
            }
         }
      }
   }

   private void WriteToSessionChannel(Result<INetworkSession, NetworkCodeError> result)
   {
      _sessionChannel.Writer.TryWrite(result);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposedState, 1) == 1) return;

      await UnbindAsync();

      _listenerSocket?.Dispose();
      await _ioQueueRegistry.DisposeAsync();
   }
}
