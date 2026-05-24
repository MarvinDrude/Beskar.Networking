using System.Net;
using System.Net.Quic;
using System.Net.Security;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Me.Memory.Results;

namespace Beskar.Networking.Transports.Quic;

/// <summary>
/// A high-performance QUIC client implementation built on native System.Net.Quic.
/// </summary>
public sealed class QuicNetworkClient(QuicTransportOptions options)
   : INetworkClient, IAsyncDisposable
{
   private readonly QuicTransportOptions _options = options;
   private readonly QuicIoQueueRegistry _ioQueueRegistry = new(options);

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
         return new QuicNetworkSession(connection, _options, _ioQueueRegistry);
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

   public async ValueTask DisposeAsync()
   {
      await _ioQueueRegistry.DisposeAsync();
   }
}
