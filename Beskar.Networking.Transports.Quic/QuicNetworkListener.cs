using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Threading.Channels;
using System.Security.Cryptography.X509Certificates;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Telemetry;

namespace Beskar.Networking.Transports.Quic;

/// <summary>
/// A high-performance QUIC listener that decouples accepted connections using a background queue channel.
/// </summary>
public sealed class QuicNetworkListener(
   EndPoint localAddress,
   QuicTransportOptions options)
   : INetworkListener
{
   private readonly EndPoint _configuredLocalAddress = localAddress;
   public EndPoint LocalAddress => _listener?.LocalEndPoint ?? _configuredLocalAddress;

   public TransportKind Transport => TransportKind.Quic;
   public bool IsBound => _listener is not null;

   private long _binds;
   private long _unbinds;
   private long _sessionsAccepted;

   public NetworkListenerStats Stats => new()
   {
      Binds = Interlocked.Read(ref _binds),
      Unbinds = Interlocked.Read(ref _unbinds),
      SessionsAccepted = Interlocked.Read(ref _sessionsAccepted)
   };

   private readonly QuicTransportOptions _options = options;
   private readonly QuicIoQueueRegistry _ioQueueRegistry = new(options);
   private X509Certificate2? _generatedCertificate;

   private QuicListener? _listener;
   private CancellationTokenSource? _acceptCts;

   private int _disposedState; // 0 = active, 1 = disposed

   private Channel<Result<INetworkSession, NetworkCodeError>> _sessionChannel =
      Channel.CreateBounded<Result<INetworkSession, NetworkCodeError>>(new BoundedChannelOptions(1024)
      {
         SingleWriter = false,
         SingleReader = true,
         FullMode = BoundedChannelFullMode.Wait
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
         _sessionChannel = Channel.CreateBounded<Result<INetworkSession, NetworkCodeError>>(
            new BoundedChannelOptions(_options.MaxPendingConnections)
            {
               SingleWriter = false,
               SingleReader = true,
               FullMode = BoundedChannelFullMode.Wait
            });

         TraceLogger.LogServerInfo("QUIC Listener: Binding socket to address {0}", LocalAddress);
         var ipEndPoint = LocalAddress as IPEndPoint
            ?? throw new ArgumentException("IPEndPoint is required for QUIC listener.", nameof(LocalAddress));
         var alpn = new SslApplicationProtocol(_options.AlpnProtocol);

         var serverAuthOptions = _options.SslServerOptions ?? new SslServerAuthenticationOptions();
         serverAuthOptions.ApplicationProtocols ??= [alpn];

         // Automatically generate a self-signed cert if not provided (convenient dev default)
         if (serverAuthOptions.ServerCertificate is null && serverAuthOptions.ServerCertificateSelectionCallback is null)
         {
            _generatedCertificate = CertificateUtility.GenerateSelfSignedCertificate();
            serverAuthOptions.ServerCertificate = _generatedCertificate;
         }

         var serverConnectionOptions = new QuicServerConnectionOptions
         {
            DefaultStreamErrorCode = _options.DefaultStreamErrorCode,
            DefaultCloseErrorCode = _options.DefaultCloseErrorCode,
            MaxInboundBidirectionalStreams = _options.MaxInboundBidirectionalStreams,
            MaxInboundUnidirectionalStreams = _options.MaxInboundUnidirectionalStreams,
            ServerAuthenticationOptions = serverAuthOptions,
            IdleTimeout = _options.IdleTimeout,
            HandshakeTimeout = _options.HandshakeTimeout
         };

         if (_options.KeepAliveInterval.HasValue)
         {
            serverConnectionOptions.KeepAliveInterval = _options.KeepAliveInterval.Value;
         }

         var listenerOptions = new QuicListenerOptions
         {
            ListenEndPoint = ipEndPoint,
            ApplicationProtocols = [alpn],
            ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(serverConnectionOptions)
         };

         _listener = await QuicListener.ListenAsync(listenerOptions, ct);
         _acceptCts = new CancellationTokenSource();

         _ = AcceptLoopAsync(_listener, _acceptCts.Token);
         TraceLogger.LogServerInfo("QUIC Listener: Successfully bound and listening on {0}", LocalAddress);

         Interlocked.Increment(ref _binds);
         TransportMetrics.RecordListenerStarted(TransportKind.Quic);
         return true;
      }
      catch (QuicException ex)
      {
         TraceLogger.LogServerError("QUIC Listener: Failed to bind to {0} (Code: {1}): {2}", LocalAddress, (int)ex.QuicError, ex.Message);
         return new NetworkCodeError((int)ex.QuicError, ex.Message);
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("QUIC Listener: Failed to bind to {0}: {1}", LocalAddress, ex.Message);
         return new NetworkCodeError(-1, ex.Message);
      }
   }
   public async ValueTask<VoidResult<NetworkCodeError>> UnbindAsync(CancellationToken ct = default)
   {
      try
      {
         TraceLogger.LogServerInfo("QUIC Listener: Unbinding and stopping listener on {0}", LocalAddress);
         if (_acceptCts is not null)
         {
            await _acceptCts.CancelAsync();
         }
         _acceptCts?.Dispose();
         _acceptCts = null;

         var cert = Interlocked.Exchange(ref _generatedCertificate, null);
         cert?.Dispose();

         var listener = Interlocked.Exchange(ref _listener, null);
         if (listener is not null)
         {
            await listener.DisposeAsync();
            TransportMetrics.RecordListenerStopped(TransportKind.Quic);
         }

         _sessionChannel.Writer.TryComplete();
         while (_sessionChannel.Reader.TryRead(out var result))
         {
            if (!result.Failed)
            {
               await result.Success.DisposeAsync();
            }
         }

         TraceLogger.LogServerInfo("QUIC Listener: Successfully unbound from {0}", LocalAddress);
         Interlocked.Increment(ref _unbinds);
         return true;
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("QUIC Listener: Error during unbind from {0}: {1}", LocalAddress, ex.Message);
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   public async ValueTask<Result<INetworkSession, NetworkCodeError>> AcceptSessionAsync(CancellationToken ct = default)
   {
      if (_listener is null)
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

   private async Task AcceptLoopAsync(QuicListener listener, CancellationToken token)
   {
      while (!token.IsCancellationRequested)
      {
         QuicConnection? quicConnection = null;
         QuicNetworkSession? session = null;
         var success = false;

         try
         {
            quicConnection = await listener.AcceptConnectionAsync(token);
            TraceLogger.LogServerInfo("QUIC Listener: Accepted connection from client {0}", quicConnection.RemoteEndPoint);
            session = new QuicNetworkSession(quicConnection, _options, _ioQueueRegistry);

            TraceLogger.LogServerInfo("QUIC Listener: Enqueuing network session {0} for client {1}", session.Id, quicConnection.RemoteEndPoint);
            Interlocked.Increment(ref _sessionsAccepted);
            await _sessionChannel.Writer.WriteAsync(session, token);

            success = true;
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

            TransportMetrics.RecordConnectionFailed(TransportKind.Quic, ex.QuicError.ToString());
            TraceLogger.LogServerError("QUIC Listener: QuicException accepting connection (Code: {0}): {1}", (int)ex.QuicError, ex.Message);
            WriteToSessionChannel(new NetworkCodeError((int)ex.QuicError, $"Listener acceptance error: {ex.Message}"));

            try { await Task.Delay(_options.AcceptExceptionDelay, token); } catch (OperationCanceledException) { break; }
         }
         catch (Exception ex)
         {
            if (token.IsCancellationRequested || _listener is null)
            {
               break;
            }

            TransportMetrics.RecordConnectionFailed(TransportKind.Quic, ex.GetType().Name);
            TraceLogger.LogServerError("QUIC Listener: Unexpected error accepting connection: {0}", ex.Message);
            WriteToSessionChannel(new NetworkCodeError(-1, $"Listener acceptance error: {ex.Message}"));

            try { await Task.Delay(_options.AcceptExceptionDelay, token); } catch (OperationCanceledException) { break; }
         }
         finally
         {
            if (!success)
            {
               if (session is not null)
               {
                  await session.DisposeAsync();
               }
               else if (quicConnection is not null)
               {
                  await quicConnection.DisposeAsync();
               }
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
      await _ioQueueRegistry.DisposeAsync();
   }
}
