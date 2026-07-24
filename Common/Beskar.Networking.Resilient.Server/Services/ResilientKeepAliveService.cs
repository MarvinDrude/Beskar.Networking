using Beskar.Networking.Protocol;

namespace Beskar.Networking.Resilient.Server.Services;

public sealed class ResilientKeepAliveService<TFrame>(ResilientServer<TFrame> server)
   : IAsyncDisposable
   where TFrame : struct, IFramingProtocol<TFrame>
{
   private readonly ResilientServer<TFrame> _server = server;

   public async Task StartAsync()
   {

   }

   public async Task StopAsync()
   {

   }

   public async ValueTask DisposeAsync()
   {

   }
}
