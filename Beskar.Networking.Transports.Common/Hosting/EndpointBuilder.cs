using System.Net;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Common.Pipelines;

namespace Beskar.Networking.Transports.Common.Hosting;

public sealed class EndpointBuilder
{
   private EndPoint? _endPoint;
   private readonly PipelineBuilder _pipelineBuilder = new();

   private Func<EndPoint, INetworkListener>? _listenerFactory;
   private Func<INetworkSession, Task>? _sessionHandler;

   public EndpointBuilder UseTransport(Func<EndPoint, INetworkListener> listenerFactory)
   {
      _listenerFactory = listenerFactory;
      return this;
   }

   public EndpointBuilder ListenOn(EndPoint endPoint)
   {
      _endPoint = endPoint;
      return this;
   }

   public EndpointBuilder ListenOnPort(int port)
   {
      _endPoint = new IPEndPoint(IPAddress.Any, port);
      return this;
   }

   public EndpointBuilder ConfigurePipeline(Action<PipelineBuilder> configure)
   {
      configure(_pipelineBuilder);
      return this;
   }

   public EndpointBuilder OnSession(Func<INetworkSession, Task> handler)
   {
      _sessionHandler = handler;
      return this;
   }

   public EndpointDefinition Build()
   {
      ArgumentNullException.ThrowIfNull(_listenerFactory);
      ArgumentNullException.ThrowIfNull(_endPoint);
      ArgumentNullException.ThrowIfNull(_sessionHandler);

      var listener = _listenerFactory(_endPoint);
      var pipeline = _pipelineBuilder.Build();

      return new EndpointDefinition(listener, _endPoint, pipeline, _sessionHandler);
   }
}
