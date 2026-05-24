using System.Net;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Common.Pipelines;

namespace Beskar.Networking.Transports.Common.Hosting;

public sealed class NetworkServerBuilder
{
   private readonly List<EndpointBuilder> _endpoints = [];

   public static NetworkServerBuilder Create()
   {
      return new NetworkServerBuilder();
   }

   public NetworkServerBuilder ConfigureServers(Action<EndpointCollection> configure)
   {
      var collection = new EndpointCollection(_endpoints);
      configure(collection);

      return this;
   }

   public NetworkServer Build()
   {
      var definitions = _endpoints.Select(b => b.Build()).ToList();
      return new NetworkServer(definitions);
   }
}
