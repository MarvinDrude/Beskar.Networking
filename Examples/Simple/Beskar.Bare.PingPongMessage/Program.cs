
/*
 * ====================================================================================
 * Bare-Metal TCP Ping-Pong Message Example
 * ====================================================================================
 *
 * Overview:
 * This example demonstrates a low-level, bare-metal server and client network setup using
 * the Beskar networking abstractions. It showcases:
 *   1. Initializing and binding a TCP server listener.
 *   2. Asynchronously accepting and dispatching incoming client sessions.
 *   3. Initiating a TCP network client connection.
 *   4. Safe, framed, bidirectional message exchange (Ping-Pong) over System.IO.Pipelines.
 *   5. Graceful connection teardown and resource disposal.
 *
 * Abstraction & Transport Agnosticism:
 * Although this demo uses TCP as the underlying transport, all high-level client and server
 * operations program against the core interfaces.
 * This design enables developers to swap the underlying transport (e.g., to QUIC or WebSockets)
 * without modifying the application code layer.
 *
 * Length-Prefixed Message Framing:
 * Because TCP is a stream-oriented protocol, it does not preserve packet boundaries (data
 * can be fragmented or consolidated during transmission). To guarantee that messages are parsed
 * safely without reading partial messages or coalescing multiple messages, this example
 * implements length-prefixed framing:
 *   - Each message is prefixed with a 4-byte big-endian integer indicating the payload length.
 *   - The receiver waits until the full payload has arrived before advancing the PipeReader
 *     past the frame boundary.
 *
 * Diagnostics & Logging:
 * This project utilizes `TraceLogger` for real-time console rendering of connection states
 * and packet details.
 *   - By default, `TraceLogger` only outputs logs in `DEBUG` builds to minimize runtime overhead.
 *   - Toggle logger visibility using `TraceLogger.IsEnabled = true;`.
 */

using System.Buffers;
using System.Buffers.Binary;
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
      var length = payload.Length;

      var memory = _stream.Transport.Output.GetMemory(4 + length);
      BinaryPrimitives.WriteInt32BigEndian(memory.Span[..4], length);

      payload.CopyTo(memory.Span[4..]);
      _stream.Transport.Output.Advance(4 + length);
      await _stream.Transport.Output.FlushAsync(ct);

      Console.WriteLine($"[Client] Sent: Ping {index}");
   }

   public async Task<string?> ReceivePongAsync(CancellationToken ct = default)
   {
      if (_stream is null) return null;

      var reader = _stream.Transport.Input;
      while (true)
      {
         var result = await reader.ReadAsync(ct);
         var buffer = result.Buffer;

         if (FrameParser.TryParseFrame(ref buffer, out var payload, out var consumedPosition))
         {
            reader.AdvanceTo(consumedPosition, buffer.End);
            var response = Encoding.UTF8.GetString(payload);

            Console.WriteLine($"[Client] Received: {response}");
            return response;
         }

         reader.AdvanceTo(buffer.Start, buffer.End);

         if (result.IsCompleted)
         {
            return null;
         }
      }
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

      var streamResult = await session.AcceptStreamAsync(ct);
      if (streamResult.Failed)
      {
         Console.WriteLine($"[Server] Failed to accept stream: {streamResult.Error.Message}");
         return;
      }

      var stream = streamResult.Success;
      var reader = stream.Transport.Input;
      var writer = stream.Transport.Output;

      try
      {
         while (!ct.IsCancellationRequested)
         {
            var readResult = await reader.ReadAsync(ct);
            var buffer = readResult.Buffer;

            var consumed = buffer.Start;
            var examined = buffer.End;

            while (FrameParser.TryParseFrame(ref buffer, out var payload, out var consumedPosition))
            {
               consumed = consumedPosition;

               var request = Encoding.UTF8.GetString(payload);
               Console.WriteLine($"[Server] Received: {request}");

               if (request.StartsWith("Ping"))
               {
                  var pongMessage = request.Replace("Ping", "Pong");
                  var pongBytes = Encoding.UTF8.GetBytes(pongMessage);
                  var length = pongBytes.Length;

                  var memory = writer.GetMemory(4 + length);
                  BinaryPrimitives.WriteInt32BigEndian(memory.Span[..4], length);

                  pongBytes.CopyTo(memory.Span[4..]);
                  writer.Advance(4 + length);

                  await writer.FlushAsync(ct);

                  Console.WriteLine($"[Server] Sent: {pongMessage}");
               }

               // Slice the buffer so the next iteration of TryParseFrame operates on the remainder
               buffer = buffer.Slice(consumedPosition);
            }

            reader.AdvanceTo(consumed, examined);

            if (readResult.IsCompleted)
            {
               break;
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

internal static class FrameParser
{
   public static bool TryParseFrame(ref ReadOnlySequence<byte> buffer, out byte[] payload, out SequencePosition consumedPosition)
   {
      payload = [];
      consumedPosition = default;

      if (buffer.Length < 4)
      {
         return false;
      }

      // Read length (4 bytes)
      Span<byte> lengthBytes = stackalloc byte[4];
      buffer.Slice(0, 4).CopyTo(lengthBytes);

      var length = BinaryPrimitives.ReadInt32BigEndian(lengthBytes);

      if (buffer.Length < 4 + length)
      {
         return false;
      }

      // Extract the payload bytes
      var payloadSequence = buffer.Slice(4, length);
      payload = payloadSequence.ToArray();

      // The position to advance to is 4 + length from the start
      consumedPosition = buffer.GetPosition(4 + length);
      return true;
   }
}
