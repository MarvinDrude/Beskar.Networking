using System.Net;
using System.Threading.Channels;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Tcp;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.Ws;

/// <summary>
/// A high-performance WebSocket server listener.
/// </summary>
public sealed class WsNetworkListener(EndPoint localAddress, WsTransportOptions options) : INetworkListener
{
   public EndPoint LocalAddress { get; } = localAddress;

   private readonly WsTransportOptions _options = options;
   private readonly TcpNetworkListener _tcpListener = new(localAddress, options.TcpOptions);

   private CancellationTokenSource? _acceptCts;
   private Task? _acceptLoopTask;

   private readonly Channel<Result<INetworkSession, NetworkCodeError>> _sessionChannel =
      Channel.CreateUnbounded<Result<INetworkSession, NetworkCodeError>>(new UnboundedChannelOptions
      {
         SingleWriter = false,
         SingleReader = true
      });

   public async ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
   {
      TraceLogger.LogServerInfo("WS Listener: Binding WebSocket listener to address {0} (Path: {1})", LocalAddress, _options.Path);
      var bindResult = await _tcpListener.BindAsync(ct);
      if (bindResult.Failed)
      {
         TraceLogger.LogServerError("WS Listener: Failed to bind TCP listener to {0}: {1}", LocalAddress, bindResult.Error.Message);
         return bindResult.Error;
      }

      _acceptCts = new CancellationTokenSource();
      _acceptLoopTask = AcceptLoopAsync(_acceptCts.Token);

      TraceLogger.LogServerInfo("WS Listener: Successfully bound and listening on {0}", LocalAddress);
      return true;
   }

   public async ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      try
      {
         TraceLogger.LogServerInfo("WS Listener: Unbinding and stopping WebSocket listener on {0}", LocalAddress);
         if (_acceptCts is not null)
         {
            await _acceptCts.CancelAsync();

            _acceptCts.Dispose();
            _acceptCts = null;
         }

         if (_acceptLoopTask is not null)
         {
            try
            {
               await _acceptLoopTask;
            }
            catch { /* Ignored */ }
            _acceptLoopTask = null;
         }

         await _tcpListener.UnbindAsync(ct);
         _sessionChannel.Writer.TryComplete();

         TraceLogger.LogServerInfo("WS Listener: Successfully unbound from {0}", LocalAddress);
         return true;
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("WS Listener: Error during unbind from {0}: {1}", LocalAddress, ex.Message);
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   public ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
   {
      try
      {
         return _sessionChannel.Reader.TryRead(out var result)
            ? ValueTask.FromResult(result)
            : Awaited();
      }
      catch (ChannelClosedException)
      {
         return ValueTask.FromResult<Result<INetworkSession, NetworkCodeError>>(
            new NetworkCodeError(-1, "Listener is unbound and the session channel is closed."));
      }

      async ValueTask<Result<INetworkSession, NetworkCodeError>> Awaited()
      {
         try
         {
            return await _sessionChannel.Reader.ReadAsync(ct);
         }
         catch (ChannelClosedException)
         {
            return new NetworkCodeError(-1, "Listener is unbound and the session channel is closed.");
         }
      }
   }

   private async Task AcceptLoopAsync(CancellationToken token)
   {
      while (!token.IsCancellationRequested)
      {
         try
         {
            var tcpSessionResult = await _tcpListener.AcceptSessionAsync(token);
            if (tcpSessionResult.Failed)
            {
               continue;
            }

            var tcpSession = tcpSessionResult.Success;
            TraceLogger.LogServerInfo("WS Listener: Accepted TCP connection from client {0}, initiating WebSocket server handshake...", tcpSession.RemoteAddress);

            _ = Task.Run(async () =>
            {
               WsNetworkSession? wsSession = null;
               try
               {
                  using var handshakeTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                  handshakeTimeoutCts.CancelAfter(TimeSpan.FromSeconds(10));

                  var tcpStreamResult = await tcpSession.AcceptStreamAsync(handshakeTimeoutCts.Token);
                  if (tcpStreamResult.Failed)
                  {
                     TraceLogger.LogServerError("WS Listener: Failed to accept TCP stream for handshake from {0}: {1}", tcpSession.RemoteAddress, tcpStreamResult.Error.Message);
                     await ((IAsyncDisposable)tcpSession).DisposeAsync();
                     return;
                  }

                  var tcpPipe = tcpStreamResult.Success.Transport;
                  var acceptKey = await WsHandshake.ServerHandshakeAsync(tcpPipe, _options, handshakeTimeoutCts.Token);

                  if (acceptKey == null)
                  {
                     TraceLogger.LogServerError("WS Listener: WebSocket server handshake failed for client {0}.", tcpSession.RemoteAddress);
                     await ((IAsyncDisposable)tcpSession).DisposeAsync();
                     return;
                  }

                  var wsPipe = new WsDuplexPipe(tcpPipe, maskOutgoing: false);
                  wsSession = new WsNetworkSession(tcpSession, wsPipe);

                  TraceLogger.LogServerInfo("WS Listener: WebSocket server handshake successfully completed for client {0}. Enqueuing session {1}", tcpSession.RemoteAddress, wsSession.Id);
                  await _sessionChannel.Writer.WriteAsync(wsSession, token);
               }
               catch (Exception ex)
               {
                  TraceLogger.LogServerError("WS Listener: Unexpected exception during WebSocket handshake for client {0}: {1}", tcpSession.RemoteAddress, ex.Message);
                  if (wsSession != null)
                  {
                     await wsSession.DisposeAsync();
                  }
                  else
                  {
                     await ((IAsyncDisposable)tcpSession).DisposeAsync();
                  }
               }
            }, token);
         }
         catch (OperationCanceledException)
         {
            break;
         }
         catch (Exception ex)
         {
            if (token.IsCancellationRequested) break;
            TraceLogger.LogServerError("WS Listener: Unexpected error in acceptance loop: {0}", ex.Message);
            _sessionChannel.Writer.TryWrite(new NetworkCodeError(-1, $"Listener acceptance error: {ex.Message}"));
         }
      }
   }
}
