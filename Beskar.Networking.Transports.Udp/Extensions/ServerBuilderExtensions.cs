using System.Net;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Transports.Udp.Extensions;

public static class ServerBuilderExtensions
{
   extension<TSelf>(IServerBuilder<TSelf> builder)
   {
      /// <summary>
      /// Configures the server to use a UDP transport listener.
      /// </summary>
      /// <param name="endpoint">The <see cref="IPEndPoint"/> to bind the UDP transport listener.</param>
      /// <param name="options">Optional UDP transport configuration options. Defaults to a new instance of <see cref="UdpTransportOptions"/> if not provided.</param>
      /// <returns>An updated instance of the server builder.</returns>
      public TSelf UseUdp(IPEndPoint endpoint, UdpTransportOptions? options = null)
      {
         options ??= new UdpTransportOptions();
         return builder.Use(new UdpNetworkListener(endpoint, options));
      }

      /// <summary>
      /// Configures the server to use a UDP transport listener.
      /// </summary>
      /// <param name="port">The port number to bind the UDP transport listener.</param>
      /// <param name="options">Optional UDP transport configuration options. Defaults to a new instance of <see cref="UdpTransportOptions"/> if not provided.</param>
      /// <returns>An updated instance of the server builder.</returns>
      public TSelf UseUdp(int port, UdpTransportOptions? options = null)
      {
         return builder.UseUdp(new IPEndPoint(IPAddress.Any, port), options);
      }
   }
}
