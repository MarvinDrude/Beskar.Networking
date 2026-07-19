using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.Udp;

/// <summary>
/// A high-performance UDP transport listener that multiplexes incoming datagrams
/// into distinct virtual client sessions.
/// </summary>
public sealed class UdpNetworkListener(
   EndPoint localAddress,
   UdpTransportOptions options)
   : INetworkListener
{
   private readonly EndPoint _configuredLocalAddress = localAddress;
   public EndPoint LocalAddress => _listenerSocket?.LocalEndPoint ?? _configuredLocalAddress;

   public TransportKind Transport => TransportKind.Udp;
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

   private readonly UdpTransportOptions _options = options;
   private readonly ConcurrentDictionary<EndPoint, UdpNetworkSession> _sessions = new();

   private Socket? _listenerSocket;
   private CancellationTokenSource? _acceptCts;
   private Task? _receiveLoopTask;
   private Task? _cleanupLoopTask;

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

         TraceLogger.LogServerInfo("UDP Listener: Binding socket to address {0}", LocalAddress);
         var socket = new Socket(LocalAddress.AddressFamily, SocketType.Dgram, ProtocolType.Udp);

         socket.SetSocketOption(SocketOptionLevel.Socket, SocketOptionName.ReuseAddress, true);
         socket.Bind(LocalAddress);

         if (_options.SendBufferSize.HasValue)
         {
            socket.SendBufferSize = _options.SendBufferSize.Value;
         }
         if (_options.ReceiveBufferSize.HasValue)
         {
            socket.ReceiveBufferSize = _options.ReceiveBufferSize.Value;
         }

         _listenerSocket = socket;
         _acceptCts = new CancellationTokenSource();

         _receiveLoopTask = Task.Run(() => ReceiveLoopAsync(socket, _acceptCts.Token));
         _cleanupLoopTask = Task.Run(() => CleanupIdleSessionsLoopAsync(_acceptCts.Token));

         TraceLogger.LogServerInfo("UDP Listener: Successfully bound and listening on {0}", LocalAddress);
         Interlocked.Increment(ref _binds);

         return new ValueTask<VoidResult<NetworkCodeError>>(true);
      }
      catch (SocketException ex)
      {
         TraceLogger.LogServerError("UDP Listener: Failed to bind to {0}: {1}", LocalAddress, ex.Message);
         return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(ex.ErrorCode, ex.Message));
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("UDP Listener: Failed to bind to {0}: {1}", LocalAddress, ex.Message);
         return new ValueTask<VoidResult<NetworkCodeError>>(new NetworkCodeError(-1, ex.Message));
      }
   }

   public async ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      try
      {
         TraceLogger.LogServerInfo("UDP Listener: Unbinding and stopping listener socket on {0}", LocalAddress);
         if (_acceptCts is not null)
         {
            await _acceptCts.CancelAsync();

            _acceptCts.Dispose();
            _acceptCts = null;
         }

         var socket = Interlocked.Exchange(ref _listenerSocket, null);
         if (socket is not null)
         {
            try
            {
               socket.Close();
            }
            catch
            {
               // Ignored
            }
            socket.Dispose();
         }

         if (_receiveLoopTask is not null)
         {
            try
            {
               await _receiveLoopTask;
            }
            catch
            {
               // Ignored
            }
            _receiveLoopTask = null;
         }

         if (_cleanupLoopTask is not null)
         {
            try
            {
               await _cleanupLoopTask;
            }
            catch
            {
               // Ignored
            }
            _cleanupLoopTask = null;
         }

         _sessionChannel.Writer.TryComplete();
         while (_sessionChannel.Reader.TryRead(out var result))
         {
            if (!result.Failed)
            {
               await result.Success.DisposeAsync();
            }
         }

         // Dispose all active sessions
         foreach (var session in _sessions.Values)
         {
            await session.DisposeAsync();
         }
         _sessions.Clear();

         TraceLogger.LogServerInfo("UDP Listener: Successfully unbound from {0}", LocalAddress);
         Interlocked.Increment(ref _unbinds);

         return true;
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("UDP Listener: Error during unbind from {0}: {1}", LocalAddress, ex.Message);
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

   private async Task ReceiveLoopAsync(Socket socket, CancellationToken token)
   {
      var buffer = new byte[65536];
      EndPoint remoteEP = new IPEndPoint(IPAddress.Any, 0);

      while (!token.IsCancellationRequested)
      {
         try
         {
            var result = await socket.ReceiveFromAsync(buffer, SocketFlags.None, remoteEP, token);
            if (result.ReceivedBytes == 0)
            {
               continue;
            }

            var senderEP = result.RemoteEndPoint;

            if (_sessions.TryGetValue(senderEP, out var session))
            {
               await session.PushIncomingDataAsync(buffer.AsMemory(0, result.ReceivedBytes));
            }
            else
            {
               TraceLogger.LogServerInfo("UDP Listener: Accepted new logical connection from client {0}", senderEP);

               var newSession = new UdpNetworkSession(
                  LocalAddress,
                  senderEP,
                  SendToAsync,
                  _options,
                  RemoveSessionAsync);

               if (_sessions.TryAdd(senderEP, newSession))
               {
                  Interlocked.Increment(ref _sessionsAccepted);
                  await _sessionChannel.Writer.WriteAsync(newSession, token);
                  await newSession.PushIncomingDataAsync(buffer.AsMemory(0, result.ReceivedBytes));
               }
               else
               {
                  // Race condition: another thread created it
                  if (_sessions.TryGetValue(senderEP, out var existingSession))
                  {
                     await existingSession.PushIncomingDataAsync(buffer.AsMemory(0, result.ReceivedBytes));
                  }
                  await newSession.DisposeAsync();
               }
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

            TraceLogger.LogServerError("UDP Listener: Socket error accepting client: {0}", ex.Message);
            _sessionChannel.Writer.TryWrite(new NetworkCodeError(ex.ErrorCode, $"Listener acceptance error: {ex.Message}"));

            try { await Task.Delay(10, token); } catch (OperationCanceledException) { break; }
         }
         catch (Exception ex)
         {
            if (token.IsCancellationRequested || _listenerSocket is null)
            {
               break;
            }

            TraceLogger.LogServerError("UDP Listener: Unexpected error accepting client: {0}", ex.Message);
            _sessionChannel.Writer.TryWrite(new NetworkCodeError(-1, $"Listener acceptance error: {ex.Message}"));

            try { await Task.Delay(10, token); } catch (OperationCanceledException) { break; }
         }
      }
   }

   private async Task CleanupIdleSessionsLoopAsync(CancellationToken token)
   {
      while (!token.IsCancellationRequested)
      {
         try
         {
            var delay = TimeSpan.FromMilliseconds(Math.Max(100, Math.Min(5000, _options.ClientSessionIdleTimeout.TotalMilliseconds / 2)));
            await Task.Delay(delay, token);

            var nowTicks = DateTimeOffset.UtcNow.Ticks;
            var idleTimeoutTicks = _options.ClientSessionIdleTimeout.Ticks;

            foreach (var session in _sessions.Values)
            {
               if (nowTicks - session.LastActivityTicks > idleTimeoutTicks)
               {
                  TraceLogger.LogServerInfo("UDP Listener: Client session {0} ({1}) idle timeout reached. Disconnecting.", session.Id, session.RemoteAddress);
                  await session.DisposeAsync();
               }
            }
         }
         catch (OperationCanceledException)
         {
            break;
         }
         catch (Exception ex)
         {
            TraceLogger.LogServerError("UDP Listener: Error cleaning up idle sessions: {0}", ex.Message);
         }
      }
   }

   private async ValueTask SendToAsync(ReadOnlyMemory<byte> data, EndPoint remoteEP)
   {
      var socket = _listenerSocket;
      if (socket is not null)
      {
         await socket.SendToAsync(data, SocketFlags.None, remoteEP);
      }
   }

   private ValueTask RemoveSessionAsync(UdpNetworkSession session)
   {
      _sessions.TryRemove(session.RemoteAddress, out _);
      return ValueTask.CompletedTask;
   }

   public async ValueTask DisposeAsync()
   {
      if (_disposed) return;
      _disposed = true;

      await UnbindAsync();
   }
}
