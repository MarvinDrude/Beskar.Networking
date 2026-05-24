using Beskar.Networking.Transports.Common.Hosting;

namespace Beskar.Networking.Transports.Ws.Extensions;

/// <summary>
/// Provides client extension methods for configuring WebSocket client transports.
/// </summary>
public static class WsClientExtensions
{
   extension(NetworkClientBuilder builder)
   {
      public NetworkClientBuilder UseWebSocket(Action<WsTransportOptions>? configure = null)
      {
         var options = new WsTransportOptions();
         configure?.Invoke(options);

         var client = new WsNetworkClient(options);
         return builder.UseConnector(async (endPoint, ct) => await client.ConnectAsync(endPoint, ct));
      }
   }
}
