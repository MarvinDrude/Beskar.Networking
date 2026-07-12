
/*
 * The examples use the TraceLogger which only logs to the console in case of a DEBUG build.
 * -> If you want less noise in between, you can disable the TraceLogger.
 * In this example, we show a very simple server setup -> client connect -> ping pong -> gracefull shutdown.
 *
 * We use TCP here but you can easily switch the underlying transport out without changing the code on top.
 */

using System.Net;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Tcp;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;

await using var server = new Server();



await server.RunAsync();

return;

internal sealed class Client
{

}

internal sealed class Server : IAsyncDisposable
{
   // Create a tcp listener on port 23_000 and any IP
   private readonly INetworkListener _listener = new TcpNetworkListener(new IPEndPoint(IPAddress.Any, 23_000), new TcpTransportOptions()
   {
      NoDelay = true,
      UseSsl = false
   });

   private bool _disposed = false;
   private readonly CancellationTokenSource _cts = new ();

   public async Task<VoidResult<StringError>> RunAsync(CancellationToken ct = default)
   {
      using var combined = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
      var combinedToken = combined.Token;

      // bind the listener to actually receive new connections
      var startResult = await _listener.BindAsync(ct);
      if (startResult.Failed) return new StringError(startResult.Error.Message);

      // just accept connections in a loop
      while (!combinedToken.IsCancellationRequested)
      {
         var sessionResult = await _listener.AcceptSessionAsync(combinedToken);
         if (sessionResult.Failed) continue;

         _ = Task.Factory.StartNew(
            () => RunClientTask(sessionResult.Success, combinedToken),
            TaskCreationOptions.PreferFairness);
      }

      // if server gets shutdown the loop ends end we just exit this method
      return true;
   }

   private async Task RunClientTask(INetworkSession session, CancellationToken ct)
   {

   }

   public async ValueTask DisposeAsync()
   {
      if (_disposed) return;
      _disposed = true;

      await _cts.CancelAsync();
      _cts.Dispose();

      await _listener.DisposeAsync();
   }
}
