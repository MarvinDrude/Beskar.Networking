using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Telemetry;

namespace Beskar.Networking.Transports.Quic;

/// <summary>
/// A high-performance QUIC client implementation built on native System.Net.Quic.
/// </summary>
public sealed class QuicNetworkClient : INetworkClient
{
   public TransportKind Transport => TransportKind.Quic;

   [MemberNotNullWhen(true, nameof(_activeSession), nameof(Session))]
   public bool IsConnected => _activeSession is not null
      && !_activeSession.SessionClosedToken.IsCancellationRequested;

   public INetworkSession? Session => _activeSession;

   public EndPoint? LocalAddress => _activeSession?.LocalAddress;
   public EndPoint? RemoteAddress => _activeSession?.RemoteAddress;

   private long _connectionsEstablished;
   private long _connectionsLost;

   public NetworkClientStats Stats => new()
   {
      ConnectionsEstablished = Interlocked.Read(ref _connectionsEstablished),
      ConnectionsLost = Interlocked.Read(ref _connectionsLost)
   };

   private readonly QuicTransportOptions _options;
   private readonly QuicIoQueueRegistry _ioQueueRegistry;
   private readonly SslClientAuthenticationOptions _clientAuthOptions;
   private readonly QuicClientConnectionOptions _clientConnectionOptions;

   private QuicNetworkSession? _activeSession;

   public QuicNetworkClient(QuicTransportOptions options)
   {
      _options = options;
      _ioQueueRegistry = new QuicIoQueueRegistry(options);
      _clientAuthOptions = options.SslClientOptions ?? new SslClientAuthenticationOptions();

      var alpn = new SslApplicationProtocol(_options.AlpnProtocol);
      _clientAuthOptions.ApplicationProtocols ??= [alpn];
      _clientAuthOptions.RemoteCertificateValidationCallback ??= (sender, cert, chain, errors) => true;

      _clientConnectionOptions = new QuicClientConnectionOptions
      {
         DefaultStreamErrorCode = _options.DefaultStreamErrorCode,
         DefaultCloseErrorCode = _options.DefaultCloseErrorCode,
         MaxInboundBidirectionalStreams = _options.MaxInboundBidirectionalStreams,
         MaxInboundUnidirectionalStreams = _options.MaxInboundUnidirectionalStreams,
         ClientAuthenticationOptions = _clientAuthOptions,
         IdleTimeout = _options.IdleTimeout,
         HandshakeTimeout = _options.HandshakeTimeout
      };

      if (_options.KeepAliveInterval.HasValue)
      {
         _clientConnectionOptions.KeepAliveInterval = _options.KeepAliveInterval.Value;
      }
   }

   /// <inheritdoc />
   public async ValueTask<Result<INetworkSession, NetworkCodeError>> ConnectAsync(
      EndPoint endPoint, CancellationToken ct = default)
   {
      if (!QuicConnection.IsSupported)
      {
         return new NetworkCodeError(-1, "QUIC is not supported on this platform.");
      }

      try
      {
         TraceLogger.LogClientInfo("QUIC ConnectAsync: Initiating QUIC connection to {0} (ALPN: {1})", endPoint, _options.AlpnProtocol);

         _clientConnectionOptions.RemoteEndPoint = endPoint;
         var connection = await QuicConnection.ConnectAsync(_clientConnectionOptions, ct);
         QuicNetworkSession? session = null;

         try
         {
            session = new QuicNetworkSession(connection, _options, _ioQueueRegistry);

            var oldSession = Interlocked.Exchange(ref _activeSession, session);
            if (oldSession is not null)
            {
               await oldSession.DisposeAsync();
            }

            Interlocked.Increment(ref _connectionsEstablished);
            session.SessionClosedToken.Register(() => Interlocked.Increment(ref _connectionsLost));

            TraceLogger.LogClientInfo("QUIC ConnectAsync: Successfully established QUIC session {0} (Remote: {1}, Local: {2})", session.Id, connection.RemoteEndPoint, connection.LocalEndPoint);
            return session;
         }
         catch
         {
            if (session is not null)
            {
               await session.DisposeAsync();
            }
            else
            {
               await connection.DisposeAsync();
            }
            throw;
         }
      }
      catch (QuicException ex)
      {
         TransportMetrics.RecordConnectionFailed(TransportKind.Quic, ex.QuicError.ToString());
         TraceLogger.LogClientError("QUIC ConnectAsync: QUIC exception connecting to {0} (Code: {1}): {2}", endPoint, (int)ex.QuicError, ex.Message);
         return new NetworkCodeError((int)ex.QuicError, ex.Message);
      }
      catch (Exception ex)
      {
         TransportMetrics.RecordConnectionFailed(TransportKind.Quic, ex.GetType().Name);
         TraceLogger.LogClientError("QUIC ConnectAsync: Unexpected exception connecting to {0}: {1}", endPoint, ex.Message);
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   public async ValueTask DisconnectAsync(CancellationToken ct = default)
   {
      var session = Interlocked.Exchange(ref _activeSession, null);
      if (session is not null)
      {
         await session.DisposeAsync();
      }
   }

   public async ValueTask DisposeAsync()
   {
      await DisconnectAsync();
      await _ioQueueRegistry.DisposeAsync();
   }
}
