using Beskar.Networking.Transports.Common.Hosting;

namespace Beskar.Networking.Transports.Tcp.Extensions;

public static class TcpClientExtensions
{
   extension(NetworkClientBuilder builder)
   {
      public NetworkClientBuilder UseTcp(Action<TcpTransportOptions>? configure = null)
      {
         var options = new TcpTransportOptions();
         configure?.Invoke(options);

         var client = new TcpNetworkClient(options);
         return builder.UseConnector(async (endPoint, ct) => await client.ConnectAsync(endPoint, ct));
      }
   }
}
