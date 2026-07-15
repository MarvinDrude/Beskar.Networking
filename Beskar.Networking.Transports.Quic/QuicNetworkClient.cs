using System.Net;
using System.Net.Quic;
using System.Net.Security;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;

namespace Beskar.Networking.Transports.Quic;

/// <summary>
/// A high-performance QUIC client implementation built on native System.Net.Quic.
/// </summary>
public sealed class QuicNetworkClient(QuicTransportOptions options)
   : INetworkClient
{
   public TransportKind Transport => TransportKind.Quic;

   private readonly QuicTransportOptions _options = options;
   private readonly QuicIoQueueRegistry _ioQueueRegistry = new(options);

   private QuicNetworkSession? _activeSession;

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
         var alpn = new SslApplicationProtocol(_options.AlpnProtocol);

         var clientAuthOptions = _options.SslClientOptions ?? new SslClientAuthenticationOptions();

         clientAuthOptions.ApplicationProtocols ??= [alpn];
         clientAuthOptions.RemoteCertificateValidationCallback ??= (sender, cert, chain, errors) => true;

         var clientOptions = new QuicClientConnectionOptions
         {
            RemoteEndPoint = endPoint,
            DefaultStreamErrorCode = _options.DefaultStreamErrorCode,
            DefaultCloseErrorCode = _options.DefaultCloseErrorCode,
            MaxInboundBidirectionalStreams = _options.MaxInboundBidirectionalStreams,
            MaxInboundUnidirectionalStreams = _options.MaxInboundUnidirectionalStreams,
            ClientAuthenticationOptions = clientAuthOptions
         };

         if (_options.KeepAliveInterval.HasValue)
         {
            clientOptions.KeepAliveInterval = _options.KeepAliveInterval.Value;
         }

         var connection = await QuicConnection.ConnectAsync(clientOptions, ct);
         var session = new QuicNetworkSession(connection, _options, _ioQueueRegistry);

         var oldSession = Interlocked.Exchange(ref _activeSession, session);
         if (oldSession is not null)
         {
            await oldSession.DisposeAsync();
         }

         TraceLogger.LogClientInfo("QUIC ConnectAsync: Successfully established QUIC session {0} (Remote: {1}, Local: {2})", session.Id, connection.RemoteEndPoint, connection.LocalEndPoint);
         return session;
      }
      catch (QuicException ex)
      {
         TraceLogger.LogClientError("QUIC ConnectAsync: QUIC exception connecting to {0} (Code: {1}): {2}", endPoint, (int)ex.QuicError, ex.Message);
         return new NetworkCodeError((int)ex.QuicError, ex.Message);
      }
      catch (Exception ex)
      {
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
