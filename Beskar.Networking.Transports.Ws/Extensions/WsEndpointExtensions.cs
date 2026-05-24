using Beskar.Networking.Transports.Common.Hosting;

namespace Beskar.Networking.Transports.Ws.Extensions;

/// <summary>
/// Provides endpoint extensions for configuring WebSocket server listener endpoints.
/// </summary>
public static class WsEndpointExtensions
{
   extension(EndpointBuilder builder)
   {
      public EndpointBuilder UseWebSocket(Action<WsTransportOptions>? configure = null)
      {
         var options = new WsTransportOptions();
         configure?.Invoke(options);

         return builder.UseTransport(endPoint => new WsNetworkListener(endPoint, options));
      }
   }
}
