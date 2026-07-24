namespace Beskar.Networking.Resilient.Server.Services;

public sealed class ResilientKeepAliveService(ResilientServer server) : IAsyncDisposable
{
   private readonly ResilientServer _server = server;

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
