using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Transports.Memory.Extensions;

public static class ClientFactoryExtensions
{
   extension<TFactory, TSelf>(TFactory)
      where TFactory : IClientFactory<TSelf>
   {
      /// <summary>
      /// Configures the client factory to use in-memory transport with optional settings.
      /// </summary>
      /// <typeparam name="TSelf">The type of the client to be configured.</typeparam>
      /// <typeparam name="TFactory">The type of the factory that creates the client.</typeparam>
      /// <param name="options">Optional configuration options for the Memory transport.</param>
      /// <returns>An instance of <typeparamref name="TSelf"/> configured for Memory transport.</returns>
      public static TSelf UseMemory(MemoryTransportOptions? options = null)
      {
         options ??= new MemoryTransportOptions();
         var client = new MemoryNetworkClient(options);

         return TFactory.Create(client);
      }
   }
}
