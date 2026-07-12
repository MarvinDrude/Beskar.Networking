
/*
 * The examples use the TraceLogger which only logs to the console in case of a DEBUG build.
 * -> If you want less noise in between, you can disable the TraceLogger.
 * In this example, we show a very simple server setup -> client connect -> ping pong -> gracefull shutdown.
 *
 * We use TCP here but you can easily switch the underlying transport out without changing the code on top.
 *
 * Important: this is a bare metal example where we need to spin up our listen tasks etc. all by ourselves
 * and manage the correct reading from the duplex pipes.
 */

using System.Buffers;
using System.Net;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Tcp;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;

var server = new Server();

// Start the server in a separate background thread / Task
var serverTask = Task.Run(async () =>
{
   try
   {
      await server.RunAsync();
   }
   catch (Exception ex)
   {
      TraceLogger.LogServerError($"Server run encountered an exception: {ex.Message}");
   }
});

// Give the server listener a moment to bind and start accepting
await Task.Delay(1000);

await using (var client = new Client())
{
   if (await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, 23_000)))
   {
      for (var i = 1; i <= 3; i++)
      {
         await client.SendPingAsync(i);
         var pong = await client.ReceivePongAsync();

         if (pong is null)
         {
            Console.WriteLine("[Client] Failed to receive pong.");
            break;
         }

         await Task.Delay(200); // small delay between pings for readability
      }

      await client.DisconnectAsync();
   }
}

// Client is finished, so shut down the server.
Console.WriteLine("[System] Shutting down server...");
await server.DisposeAsync();

// Wait for server task to finish processing/looping
try
{
   await serverTask;
}
catch (OperationCanceledException)
{
   // Expected cancellation
}

Console.WriteLine("[System] Ping pong example finished successfully.");
return;

internal sealed class Client : IAsyncDisposable
{
   private readonly TcpNetworkClient _client = new(new TcpTransportOptions()
   {
      NoDelay = true,
      UseSsl = false
   });

   private INetworkSession? _session;
   private INetworkStream? _stream;

   public async Task<bool> ConnectAsync(IPEndPoint endPoint, CancellationToken ct = default)
   {
      var connectResult = await _client.ConnectAsync(endPoint, ct);
      if (connectResult.Failed)
      {
         Console.WriteLine($"[Client] Connection failed: {connectResult.Error.Message}");
         return false;
      }

      _session = connectResult.Success;

      var streamResult = await _session.AcceptStreamAsync(ct);
      if (streamResult.Failed)
      {
         Console.WriteLine($"[Client] Stream accept failed: {streamResult.Error.Message}");
         return false;
      }

      _stream = streamResult.Success;
      return true;
   }

   public async Task SendPingAsync(int index, CancellationToken ct = default)
   {
      if (_stream is null) return;

      var payload = Encoding.UTF8.GetBytes($"Ping {index}");
      await _stream.Transport.Output.WriteAsync(payload, ct);
      await _stream.Transport.Output.FlushAsync(ct);

      Console.WriteLine($"[Client] Sent: Ping {index}");
   }

   public async Task<string?> ReceivePongAsync(CancellationToken ct = default)
   {
      if (_stream is null) return null;

      var readResult = await _stream.Transport.Input.ReadAsync(ct);
      var buffer = readResult.Buffer;

      if (buffer.IsEmpty && readResult.IsCompleted)
      {
         return null;
      }

      var response = Encoding.UTF8.GetString(buffer.ToArray());
      _stream.Transport.Input.AdvanceTo(buffer.End);

      Console.WriteLine($"[Client] Received: {response}");
      return response;
   }

   public async Task DisconnectAsync(CancellationToken ct = default)
   {
      Console.WriteLine("[Client] Disconnecting...");
      await _client.DisconnectAsync(ct);
   }

   public async ValueTask DisposeAsync()
   {
      if (_session is not null)
      {
         await _session.DisposeAsync();
      }
      await _client.DisposeAsync();
   }
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

      try
      {
         // just accept connections in a loop
         while (!combinedToken.IsCancellationRequested)
         {
            var sessionResult = await _listener.AcceptSessionAsync(combinedToken);
            if (sessionResult.Failed) continue;

            _ = Task.Factory.StartNew(
               () => RunClientTask(sessionResult.Success, combinedToken),
               TaskCreationOptions.PreferFairness);
         }
      }
      catch (Exception) when (combinedToken.IsCancellationRequested)
      {
         // Ignore exception on cancellation
      }

      // if server gets shutdown the loop ends end we just exit this method
      return true;
   }

   private async Task RunClientTask(INetworkSession session, CancellationToken ct)
   {
      Console.WriteLine($"[Server] Client connected: {session.RemoteAddress}");
      TraceLogger.LogServerInfo($"Client connected: {session.RemoteAddress}");

      var streamResult = await session.AcceptStreamAsync(ct);
      if (streamResult.Failed)
      {
         Console.WriteLine($"[Server] Failed to accept stream: {streamResult.Error.Message}");
         return;
      }

      var stream = streamResult.Success;

      try
      {
         while (!ct.IsCancellationRequested)
         {
            var readResult = await stream.Transport.Input.ReadAsync(ct);
            var buffer = readResult.Buffer;

            if (buffer.IsEmpty && readResult.IsCompleted)
            {
               break;
            }

            var request = Encoding.UTF8.GetString(buffer.ToArray());
            stream.Transport.Input.AdvanceTo(buffer.End);

            Console.WriteLine($"[Server] Received: {request}");

            if (request.StartsWith("Ping"))
            {
               var pongMessage = request.Replace("Ping", "Pong");
               var payload = Encoding.UTF8.GetBytes(pongMessage);

               await stream.Transport.Output.WriteAsync(payload, ct);
               await stream.Transport.Output.FlushAsync(ct);

               Console.WriteLine($"[Server] Sent: {pongMessage}");
            }
         }
      }
      catch (Exception ex)
      {
         Console.WriteLine($"[Server] Error in client task: {ex.Message}");
      }
      finally
      {
         await session.DisposeAsync();
         Console.WriteLine("[Server] Session disposed.");
      }
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
