using System.IO.Pipelines;

namespace Beskar.Networking.Transports.Common.Pipelines;

public sealed class PipelineBuilder
{
   private readonly List<NetworkMiddlewareDelegate> _components = new();

   public PipelineBuilder Use(NetworkMiddlewareDelegate middleware)
   {
      _components.Add(middleware);
      return this;
   }

   public NetworkMiddlewareDelegate Build()
   {
      return (pipe, finalNext) =>
      {
         var index = 0;
         Task Next()
         {
            if (index < _components.Count)
            {
               var middleware = _components[index++];
               return middleware(pipe, Next);
            }
            return finalNext();
         }
         return Next();
      };
   }
}
