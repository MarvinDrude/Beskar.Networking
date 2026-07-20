using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Transports.Uds.Extensions;

public static class ClientFactoryExtensions
{
   extension<TFactory, TSelf>(TFactory)
      where TFactory : IClientFactory<TSelf>
   {
      /// <summary>
      /// Configures the client factory to use Unix Domain Sockets (UDS) transport with optional transport settings.
      /// </summary>
      /// <typeparam name="TSelf">The type of the client to be configured.</typeparam>
      /// <typeparam name="TFactory">The type of the factory that creates the client.</typeparam>
      /// <param name="options">Optional configuration options for the UDS transport. Uses default options if not provided.</param>
      /// <returns>An instance of <typeparamref name="TSelf"/> configured for UDS transport.</returns>
      public static TSelf UseUds(UdsTransportOptions? options = null)
      {
         options ??= new UdsTransportOptions();
         var client = new UdsNetworkClient(options);

         return TFactory.Create(client);
      }
   }
}
