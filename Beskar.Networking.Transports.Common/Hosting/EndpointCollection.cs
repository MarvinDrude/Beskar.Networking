using System.Net;

namespace Beskar.Networking.Transports.Common.Hosting;

public sealed class EndpointCollection
{
   private readonly List<EndpointBuilder> _endpoints;

   internal EndpointCollection(List<EndpointBuilder> endpoints)
   {
      _endpoints = endpoints;
   }

   public void ListenAnyIP(int port, Action<EndpointBuilder>? configure = null)
   {
      Listen(new IPEndPoint(IPAddress.Any, port), configure);
   }

   public void ListenLocalhost(int port, Action<EndpointBuilder>? configure = null)
   {
      Listen(new IPEndPoint(IPAddress.Loopback, port), configure);
   }

   public void Listen(EndPoint endPoint, Action<EndpointBuilder>? configure = null)
   {
      var builder = new EndpointBuilder();
      builder.ListenOn(endPoint);

      configure?.Invoke(builder);
      _endpoints.Add(builder);
   }
}
