using Beskar.Networking.Transports.Common.Hosting;

namespace Beskar.Networking.Transports.Tcp;

public static class TcpEndpointExtensions
{
   extension(EndpointBuilder builder)
   {
      public EndpointBuilder UseTcp(Action<TcpTransportOptions>? configure = null)
      {
         var options = new TcpTransportOptions();
         configure?.Invoke(options);

         return builder.UseTransport(endPoint => new TcpNetworkListener(endPoint, options));
      }
   }
}
