using System.Net;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Common.Pipelines;

namespace Beskar.Networking.Transports.Common.Hosting;

public sealed class EndpointDefinition(
   INetworkListener listener,
   EndPoint endPoint,
   NetworkMiddlewareDelegate pipeline,
   Func<INetworkSession, Task> sessionHandler)
{
   public INetworkListener Listener { get; } = listener;

   public EndPoint EndPoint { get; } = endPoint;

   public NetworkMiddlewareDelegate Pipeline { get; } = pipeline;

   public Func<INetworkSession, Task> SessionHandler { get; } = sessionHandler;
}
