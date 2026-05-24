using Beskar.Networking.Transports.Common.Hosting;

namespace Beskar.Networking.Transports.Quic.Extensions;

/// <summary>
/// Hosting extensions for registering QUIC endpoint listeners.
/// </summary>
public static class QuicEndpointExtensions
{
   extension(EndpointBuilder builder)
   {
      /// <summary>
      /// Configures the endpoint to listen using QUIC transport.
      /// </summary>
      public EndpointBuilder UseQuic(Action<QuicTransportOptions>? configure = null)
      {
         var options = new QuicTransportOptions();
         configure?.Invoke(options);

         return builder.UseTransport(endPoint => new QuicNetworkListener(endPoint, options));
      }
   }
}
