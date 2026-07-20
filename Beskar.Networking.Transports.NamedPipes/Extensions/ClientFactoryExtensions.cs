using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Transports.NamedPipes.Extensions;

public static class ClientFactoryExtensions
{
   extension<TFactory, TSelf>(TFactory)
      where TFactory : IClientFactory<TSelf>
   {
      /// <summary>
      /// Configures the client factory to use Named Pipes transport with optional transport settings.
      /// </summary>
      /// <typeparam name="TSelf">The type of the client to be configured.</typeparam>
      /// <typeparam name="TFactory">The type of the factory that creates the client.</typeparam>
      /// <param name="options">Optional configuration options for the Named Pipes transport. Uses default options if not provided.</param>
      /// <returns>An instance of <typeparamref name="TSelf"/> configured for Named Pipes transport.</returns>
      public static TSelf UseNamedPipes(NamedPipeTransportOptions? options = null)
      {
         options ??= new NamedPipeTransportOptions();
         var client = new NamedPipeNetworkClient(options);

         return TFactory.Create(client);
      }
   }
}
