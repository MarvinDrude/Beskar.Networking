using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Transports.Udp.Extensions;

public static class ClientFactoryExtensions
{
   extension<TFactory, TSelf>(TFactory)
      where TFactory : IClientFactory<TSelf>
   {
      /// <summary>
      /// Configures the client factory to use UDP transport with optional transport settings.
      /// </summary>
      /// <typeparam name="TSelf">The type of the client to be configured.</typeparam>
      /// <typeparam name="TFactory">The type of the factory that creates the client.</typeparam>
      /// <param name="options">Optional configuration options for the UDP transport. Uses default options if not provided.</param>
      /// <returns>An instance of <typeparamref name="TSelf"/> configured for UDP transport.</returns>
      public static TSelf UseUdp(UdpTransportOptions? options = null)
      {
         options ??= new UdpTransportOptions();
         var client = new UdpNetworkClient(options);

         return TFactory.Create(client);
      }
   }
}
