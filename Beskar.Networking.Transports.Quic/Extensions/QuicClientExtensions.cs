using Beskar.Networking.Transports.Common.Hosting;

namespace Beskar.Networking.Transports.Quic.Extensions;

/// <summary>
/// Hosting extensions for registering QUIC client connectors.
/// </summary>
public static class QuicClientExtensions
{
   extension(NetworkClientBuilder builder)
   {
      /// <summary>
      /// Configures the client to connect using QUIC transport.
      /// </summary>
      public NetworkClientBuilder UseQuic(Action<QuicTransportOptions>? configure = null)
      {
         var options = new QuicTransportOptions();
         configure?.Invoke(options);

         var client = new QuicNetworkClient(options);
         return builder.UseConnector(async (endPoint, ct) => await client.ConnectAsync(endPoint, ct));
      }
   }
}
