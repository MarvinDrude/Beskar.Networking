using System.IO.Pipelines;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;

namespace Beskar.Networking.Transports.Uds;

/// <summary>
/// A high-performance UDS transport listener that manages accepted connections
/// using a non-blocking background queue.
/// </summary>
public sealed class UdsNetworkListener(
   EndPoint localAddress,
   UdsTransportOptions options)
   : INetworkListener
{
   private readonly EndPoint _configuredLocalAddress = localAddress;
   public EndPoint LocalAddress => _configuredLocalAddress;

   public TransportKind Transport => TransportKind.UnixDomainSocket;
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

   private readonly UdsTransportOptions _options = options;
   private readonly UdsIoQueueRegistry _ioQueueRegistry = new(options);

   private Socket? _listenerSocket;
   private CancellationTokenSource? _acceptCts;
   private SemaphoreSlim? _handshakeSemaphore;

   private bool _disposed;

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

         if (LocalAddress is not UnixDomainSocketEndPoint udsEndPoint)
         {
            return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(-1, "LocalAddress must be a UnixDomainSocketEndPoint."));
         }

         var socketPath = udsEndPoint.ToString();
         if (socketPath.Length > 104)
         {
            throw new ArgumentException($"Unix Domain Socket path '{socketPath}' exceeds the maximum allowed length of 104 characters (path length: {socketPath.Length}).");
         }

         TraceLogger.LogServerInfo("UDS Listener: Binding socket to file path {0}", socketPath);

         // Clean up existing socket file if it was left behind
         if (File.Exists(socketPath))
         {
            try
            {
               File.Delete(socketPath);
            }
            catch (Exception ex)
            {
               TraceLogger.LogServerError("UDS Listener: Failed to clean up existing socket file {0}: {1}", socketPath, ex.Message);
            }
         }

         var socket = new Socket(AddressFamily.Unix, SocketType.Stream, ProtocolType.Unspecified);
         socket.Bind(udsEndPoint);
         socket.Listen(_options.Backlog);

         _listenerSocket = socket;
         _acceptCts = new CancellationTokenSource();
         _handshakeSemaphore = new SemaphoreSlim(_options.MaxConcurrentHandshakes);

         _ = AcceptLoopAsync(socket, _acceptCts.Token);
         TraceLogger.LogServerInfo("UDS Listener: Successfully bound and listening on {0}", socketPath);
         Interlocked.Increment(ref _binds);

         return new ValueTask<VoidResult<NetworkCodeError>>(true);
      }
      catch (SocketException ex)
      {
         TraceLogger.LogServerError("UDS Listener: Failed to bind to UDS path: {0}", ex.Message);
         return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(ex.ErrorCode, ex.Message));
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("UDS Listener: Failed to bind: {0}", ex.Message);
         return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(-1, ex.Message));
      }
   }

   public async ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      try
      {
         TraceLogger.LogServerInfo("UDS Listener: Unbinding and stopping listener socket");
         if (_acceptCts is not null)
         {
            await _acceptCts.CancelAsync();

            _acceptCts.Dispose();
            _acceptCts = null;
         }

         var socket = Interlocked.Exchange(ref _listenerSocket, null);
         socket?.Close();
         socket?.Dispose();

         if (LocalAddress is UnixDomainSocketEndPoint udsEndPoint)
         {
            var socketPath = udsEndPoint.ToString();
            if (File.Exists(socketPath))
            {
               try
               {
                  File.Delete(socketPath);
               }
               catch (Exception ex)
               {
                  TraceLogger.LogServerError("UDS Listener: Failed to delete socket file {0} on unbind: {1}", socketPath, ex.Message);
               }
            }
         }

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

         TraceLogger.LogServerInfo("UDS Listener: Successfully unbound");
         Interlocked.Increment(ref _unbinds);

         return true;
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("UDS Listener: Error during unbind: {0}", ex.Message);
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
               if (_options.SendBufferSize.HasValue)
               {
                  clientSocket.SendBufferSize = _options.SendBufferSize.Value;
               }
               if (_options.ReceiveBufferSize.HasValue)
               {
                  clientSocket.ReceiveBufferSize = _options.ReceiveBufferSize.Value;
               }
               if (_options.LingerState is not null)
               {
                  clientSocket.LingerState = _options.LingerState;
               }

               var localEndPoint = clientSocket.LocalEndPoint ?? LocalAddress;
               var remoteEndPoint = clientSocket.RemoteEndPoint ?? LocalAddress;

               TraceLogger.LogServerInfo("UDS Listener: Accepted connection from UDS client");
               _ = Task.Run(async () =>
               {
                  try
                  {
                     // ReSharper disable once AccessToDisposedClosure
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
               TraceLogger.LogServerError("UDS Listener: Error configuring accepted UDS socket: {0}", ex.Message);
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

            TraceLogger.LogServerError("UDS Listener: Socket error accepting client: {0}", ex.Message);
            WriteToSessionChannel(new NetworkCodeError(ex.ErrorCode, $"Listener acceptance error: {ex.Message}"));

            try { await Task.Delay(_options.AcceptExceptionDelay, token); } catch (OperationCanceledException) { break; }
         }
         catch (Exception ex)
         {
            if (token.IsCancellationRequested || _listenerSocket is null)
            {
               break;
            }

            TraceLogger.LogServerError("UDS Listener: Unexpected error accepting client: {0}", ex.Message);
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
      IDuplexPipe? connection = null;
      var success = false;

      try
      {
         connection = _ioQueueRegistry.Create(socket);
         var session = new UdsNetworkSession(localEndPoint, remoteEndPoint, connection, _ioQueueRegistry.ReturnAsync);

         TraceLogger.LogServerInfo("UDS Listener: Enqueuing network session {0}", session.Id);
         Interlocked.Increment(ref _sessionsAccepted);

         await _sessionChannel.Writer.WriteAsync(session, token);
         success = true;
      }
      catch (OperationCanceledException)
      {
         TraceLogger.LogServerError("UDS Listener: Connection processing cancelled");
      }
      catch (SocketException ex)
      {
         TraceLogger.LogServerError("UDS Listener: Socket error during connection setup: {0}", ex.Message);
         WriteToSessionChannel(new NetworkCodeError(ex.ErrorCode, ex.Message));
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("UDS Listener: Unexpected error during connection setup: {0}", ex.Message);
         WriteToSessionChannel(new NetworkCodeError(-1, ex.Message));
      }
      finally
      {
         if (!success)
         {
            if (connection is not null)
            {
               await _ioQueueRegistry.ReturnAsync(connection);
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
      if (_disposed) return;
      _disposed = true;

      await UnbindAsync();

      _listenerSocket?.Dispose();
      await _ioQueueRegistry.DisposeAsync();
   }
}
