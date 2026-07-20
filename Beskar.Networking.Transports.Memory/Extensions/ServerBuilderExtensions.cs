using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Transports.Memory.Extensions;

public static class ServerBuilderExtensions
{
   extension<TSelf>(IServerBuilder<TSelf> builder)
   {
      /// <summary>
      /// Configures the server to use an in-memory transport listener.
      /// </summary>
      /// <param name="endpoint">The <see cref="MemoryEndPoint"/> to bind the in-memory transport listener.</param>
      /// <param name="options">Optional Memory transport configuration options.</param>
      /// <returns>An updated instance of the server builder.</returns>
      public TSelf UseMemory(MemoryEndPoint endpoint, MemoryTransportOptions? options = null)
      {
         options ??= new MemoryTransportOptions();
         return builder.Use(new MemoryNetworkListener(endpoint, options));
      }

      /// <summary>
      /// Configures the server to use an in-memory transport listener.
      /// </summary>
      /// <param name="address">The address name to bind the in-memory transport listener.</param>
      /// <param name="options">Optional Memory transport configuration options.</param>
      /// <returns>An updated instance of the server builder.</returns>
      public TSelf UseMemory(string address, MemoryTransportOptions? options = null)
      {
         return builder.UseMemory(new MemoryEndPoint(address), options);
      }
   }
}
