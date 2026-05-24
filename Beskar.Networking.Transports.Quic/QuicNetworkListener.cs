using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Threading.Channels;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Me.Memory.Results;

namespace Beskar.Networking.Transports.Quic;

/// <summary>
/// A high-performance QUIC listener that decouples accepted connections using a background queue channel.
/// </summary>
public sealed class QuicNetworkListener(
   EndPoint localAddress,
   QuicTransportOptions options)
   : INetworkListener
{
   public EndPoint LocalAddress { get; } = localAddress;

   private readonly QuicTransportOptions _options = options;
   private readonly QuicIoQueueRegistry _ioQueueRegistry = new(options);

   private QuicListener? _listener;
   private CancellationTokenSource? _acceptCts;

   private readonly Channel<Result<INetworkSession, NetworkCodeError>> _sessionChannel =
      Channel.CreateUnbounded<Result<INetworkSession, NetworkCodeError>>(new UnboundedChannelOptions
      {
         SingleWriter = false,
         SingleReader = true
      });

   /// <inheritdoc />
   public async ValueTask<VoidResult<NetworkCodeError>> BindAsync(CancellationToken ct = default)
   {
      if (!QuicListener.IsSupported)
      {
         return new NetworkCodeError(-1, "QUIC is not supported on this platform.");
      }

      try
      {
         var ipEndPoint = LocalAddress as IPEndPoint ?? throw new ArgumentException("IPEndPoint is required for QUIC listener.", nameof(LocalAddress));
         var alpn = new SslApplicationProtocol(_options.AlpnProtocol);

         var serverAuthOptions = _options.SslServerOptions ?? new SslServerAuthenticationOptions();
         serverAuthOptions.ApplicationProtocols ??= [alpn];

         // Automatically generate a self-signed cert if not provided (convenient dev default)
         if (serverAuthOptions.ServerCertificate is null && serverAuthOptions.ServerCertificateSelectionCallback is null)
         {
            serverAuthOptions.ServerCertificate = CertificateUtility.GenerateSelfSignedCertificate();
         }

         var listenerOptions = new QuicListenerOptions
         {
            ListenEndPoint = ipEndPoint,
            ApplicationProtocols = [alpn],
            ConnectionOptionsCallback = (connection, helloInfo, token) =>
            {
               var serverOptions = new QuicServerConnectionOptions
               {
                  DefaultStreamErrorCode = _options.DefaultStreamErrorCode,
                  DefaultCloseErrorCode = _options.DefaultCloseErrorCode,
                  MaxInboundBidirectionalStreams = _options.MaxInboundBidirectionalStreams,
                  MaxInboundUnidirectionalStreams = _options.MaxInboundUnidirectionalStreams,
                  ServerAuthenticationOptions = serverAuthOptions
               };

               if (_options.KeepAliveInterval.HasValue)
               {
                  serverOptions.KeepAliveInterval = _options.KeepAliveInterval.Value;
               }

               return ValueTask.FromResult(serverOptions);
            }
         };

         _listener = await QuicListener.ListenAsync(listenerOptions, ct);
         _acceptCts = new CancellationTokenSource();

         _ = AcceptLoopAsync(_listener, _acceptCts.Token);
         return true;
      }
      catch (QuicException ex)
      {
         return new NetworkCodeError((int)ex.QuicError, ex.Message);
      }
      catch (Exception ex)
      {
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   /// <inheritdoc />
   public async ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      try
      {
         _acceptCts?.Cancel();
         _acceptCts?.Dispose();
         _acceptCts = null;

         var listener = Interlocked.Exchange(ref _listener, null);
         if (listener is not null)
         {
            await listener.DisposeAsync();
         }

         _sessionChannel.Writer.TryComplete();

         return true;
      }
      catch (Exception ex)
      {
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   /// <inheritdoc />
   public ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
   {
      if (_listener is null)
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

   private async Task AcceptLoopAsync(QuicListener listener, CancellationToken token)
   {
      while (!token.IsCancellationRequested)
      {
         try
         {
            var quicConnection = await listener.AcceptConnectionAsync(token);
            var session = new QuicNetworkSession(quicConnection, _options, _ioQueueRegistry);

            await _sessionChannel.Writer.WriteAsync(session, token);
         }
         catch (OperationCanceledException)
         {
            break;
         }
         catch (QuicException ex)
         {
            if (token.IsCancellationRequested || _listener is null)
            {
               break;
            }
            WriteToSessionChannel(new NetworkCodeError((int)ex.QuicError, $"Listener acceptance error: {ex.Message}"));
         }
         catch (Exception ex)
         {
            if (token.IsCancellationRequested || _listener is null)
            {
               break;
            }
            WriteToSessionChannel(new NetworkCodeError(-1, $"Listener acceptance error: {ex.Message}"));
         }
      }
   }

   private void WriteToSessionChannel(Result<INetworkSession, NetworkCodeError> result)
   {
      _sessionChannel.Writer.TryWrite(result);
   }
}
