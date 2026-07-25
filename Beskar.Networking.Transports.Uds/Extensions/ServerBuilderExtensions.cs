using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Transports.Uds.Extensions;

public static class ServerBuilderExtensions
{
   extension<TSelf>(IServerBuilder<TSelf> builder)
   {
      /// <summary>
      /// Configures the server to use a Unix Domain Sockets (UDS) transport listener.
      /// </summary>
      /// <param name="endpoint">The <see cref="UnixDomainSocketEndPoint"/> to bind the UDS transport listener.</param>
      /// <param name="options">Optional UDS transport configuration options. Defaults to a new instance of <see cref="UdsTransportOptions"/> if not provided.</param>
      /// <returns>An updated instance of the server builder.</returns>
      public TSelf UseUds(UnixDomainSocketEndPoint endpoint, UdsTransportOptions? options = null)
      {
         options ??= new UdsTransportOptions();
         return builder.Use(new UdsNetworkListener(endpoint, options));
      }

      /// <summary>
      /// Configures the server to use a Unix Domain Sockets (UDS) transport listener.
      /// </summary>
      /// <param name="socketPath">The Unix Domain Socket file path to bind the UDS transport listener.</param>
      /// <param name="options">Optional UDS transport configuration options. Defaults to a new instance of <see cref="UdsTransportOptions"/> if not provided.</param>
      /// <returns>An updated instance of the server builder.</returns>
      public TSelf UseUds(string socketPath, UdsTransportOptions? options = null)
      {
         return builder.UseUds(new UnixDomainSocketEndPoint(socketPath), options);
      }
   }
}
