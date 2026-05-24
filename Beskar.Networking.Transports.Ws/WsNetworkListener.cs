using System.Net;
using System.Threading.Channels;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Tcp;
using Me.Memory.Results;

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
      var bindResult = await _tcpListener.BindAsync(ct);
      if (bindResult.Failed)
      {
         return bindResult.Error;
      }

      _acceptCts = new CancellationTokenSource();
      _acceptLoopTask = AcceptLoopAsync(_acceptCts.Token);

      return true;
   }

   public async ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      try
      {
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

         return true;
      }
      catch (Exception ex)
      {
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

            _ = Task.Run(async () =>
            {
               try
               {
                  using var handshakeTimeoutCts = CancellationTokenSource.CreateLinkedTokenSource(token);
                  handshakeTimeoutCts.CancelAfter(TimeSpan.FromSeconds(10));

                  var tcpStreamResult = await tcpSession.AcceptStreamAsync(handshakeTimeoutCts.Token);
                  if (tcpStreamResult.Failed)
                  {
                     await ((IAsyncDisposable)tcpSession).DisposeAsync();
                     return;
                  }

                  var tcpPipe = tcpStreamResult.Success.Transport;
                  var acceptKey = await WsHandshake.ServerHandshakeAsync(tcpPipe, _options, handshakeTimeoutCts.Token);

                  if (acceptKey == null)
                  {
                     await ((IAsyncDisposable)tcpSession).DisposeAsync();
                     return;
                  }

                  var wsPipe = new WsDuplexPipe(tcpPipe, maskOutgoing: false);
                  var wsSession = new WsNetworkSession(tcpSession, wsPipe);

                  await _sessionChannel.Writer.WriteAsync(wsSession, token);
               }
               catch
               {
                  await ((IAsyncDisposable)tcpSession).DisposeAsync();
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
            _sessionChannel.Writer.TryWrite(new NetworkCodeError(-1, $"Listener acceptance error: {ex.Message}"));
         }
      }
   }
}
