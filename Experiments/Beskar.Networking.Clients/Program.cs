using System;
using System.Buffers;
using System.Collections.Generic;
using System.Diagnostics;
using System.Net;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Tcp;
using Beskar.Memory.Results;

namespace Beskar.Networking.Clients;

public static class Program
{
   private const int ClientCount = 10000;
   private const int ServerPort = 9005;

   private static long _connectedClients;
   private static long _failedConnections;

   private static long _serverPingsReceived;
   private static long _clientPongsReceived;

   public static async Task Main(string[] args)
   {
      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.WriteLine("             BESKAR TCP 10K CLIENTS PING-PONG BENCHMARK           ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      var endPoint = new IPEndPoint(IPAddress.Loopback, ServerPort);

      // 1. Optimize options to handle 10k connections in loopback with low memory footprint
      var serverOptions = new TcpTransportOptions
      {
         Backlog = 15000,
         MaxPendingConnections = 15000,
         MaxConcurrentHandshakes = 15000,
         SendBufferSize = 8 * 1024,    // 8 KB buffer instead of default 512 KB
         ReceiveBufferSize = 8 * 1024, // 8 KB buffer instead of default 512 KB
         NoDelay = true
      };

      var clientOptions = new TcpTransportOptions
      {
         SendBufferSize = 8 * 1024,
         ReceiveBufferSize = 8 * 1024,
         NoDelay = true
      };

      using var cts = new CancellationTokenSource();
      var token = cts.Token;

      // 2. Start TCP Listener
      var listener = new TcpNetworkListener(endPoint, serverOptions);
      var bindResult = await listener.BindAsync(token);
      if (bindResult.Failed)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"[Server] Failed to bind: {bindResult.Error.Message}");
         Console.ResetColor();
         return;
      }

      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"[Server] Bound to {listener.LocalAddress}");
      Console.ResetColor();

      // Server Accept Loop
      var serverTask = Task.Run(async () =>
      {
         var sessionTasks = new List<Task>();
         try
         {
            while (!token.IsCancellationRequested)
            {
               var acceptResult = await listener.AcceptSessionAsync(token);
               if (acceptResult.Failed) break;

               var session = acceptResult.Success!;

               // Avoid Task.Run delegate dispatch overhead - execute directly and let it yield
               var t = HandleServerSessionAsync(session, token);
               sessionTasks.Add(t);
            }
         }
         catch (Exception)
         {
            // Exit loop
         }
         finally
         {
            await Task.WhenAll(sessionTasks);
         }
      });

      // 3. Establish 10k Client Connections in Batches to prevent socket starvation
      Console.WriteLine($"Connecting {ClientCount:N0} clients to server...");
      var stopwatch = Stopwatch.StartNew();

      var clients = new List<TcpNetworkClient>(ClientCount);
      var clientSessions = new List<INetworkSession>(ClientCount);
      var clientTasks = new List<Task>();

      const int batchSize = 100;
      for (var i = 0; i < ClientCount; i += batchSize)
      {
         var connectTasks = new List<Task<Result<INetworkSession, NetworkCodeError>>>();
         for (var j = 0; j < batchSize && (i + j) < ClientCount; j++)
         {
            var client = new TcpNetworkClient(clientOptions);
            clients.Add(client);
            connectTasks.Add(client.ConnectAsync(endPoint, token).AsTask());
         }

         var results = await Task.WhenAll(connectTasks);
         foreach (var res in results)
         {
            if (res.IsSuccess)
            {
               clientSessions.Add(res.Success!);
               Interlocked.Increment(ref _connectedClients);
            }
            else
            {
               Interlocked.Increment(ref _failedConnections);
            }
         }

         // Monitor connection progression
         if (i > 0 && i % 2000 == 0)
         {
            Console.WriteLine($" -> {i:N0} clients connection attempts completed...");
         }

         await Task.Delay(10, token); // Small pacing delay
      }

      stopwatch.Stop();
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"Setup Complete in {stopwatch.Elapsed.TotalSeconds:F2}s!");
      Console.WriteLine($" -> Connected: {_connectedClients:N0}");
      Console.WriteLine($" -> Failed:    {_failedConnections:N0}");
      Console.ResetColor();

      if (_connectedClients == 0)
      {
         Console.WriteLine("Failed to connect any clients. Aborting test.");
         return;
      }

      // 4. Start Ping-Pong Loops for connected clients
      Console.WriteLine("\nStarting load test (clients pinging every 1 second)...");
      stopwatch.Restart();

      foreach (var session in clientSessions)
      {
         clientTasks.Add(RunClientPingPongLoopAsync(session, token));
      }

      // 5. Monitor and Report Stats in Loop
      var reportingCts = new CancellationTokenSource();
      var reportTask = Task.Run(async () =>
      {
         var prevPings = Interlocked.Read(ref _serverPingsReceived);
         var reportStopwatch = Stopwatch.StartNew();

         while (!reportingCts.Token.IsCancellationRequested)
         {
            await Task.Delay(1000, reportingCts.Token);

            var elapsed = reportStopwatch.Elapsed.TotalSeconds;
            reportStopwatch.Restart();

            var currentPings = Interlocked.Read(ref _serverPingsReceived);
            var diffPings = currentPings - prevPings;
            prevPings = currentPings;

            var pingsPerSecond = diffPings / elapsed;
            var currentMemoryMb = GC.GetTotalMemory(forceFullCollection: false) / (1024.0 * 1024.0);
            var activeThreads = Process.GetCurrentProcess().Threads.Count;

            Console.WriteLine($"[{stopwatch.Elapsed:mm\\:ss}] Connected: {_connectedClients:N0} | Pings/s: {pingsPerSecond:F0} | Total Pings: {currentPings:N0} | Process Memory: {currentMemoryMb:F1} MB | Threads: {activeThreads}");
         }
      });

      // Run load test for 15 seconds
      await Task.Delay(15000);

      // 6. Graceful Tear Down
      Console.ForegroundColor = ConsoleColor.Yellow;
      Console.WriteLine("\nTearing down connections...");
      Console.ResetColor();

      reportingCts.Cancel();
      await cts.CancelAsync(); // Stop loops

      try
      {
         await Task.WhenAll(clientTasks);
         await serverTask;
      }
      catch (Exception)
      {
         // Ignored on cancellation
      }

      await reportTask;

      // Dispose clients and listener
      var disposeTasks = new List<Task>();
      foreach (var session in clientSessions)
         disposeTasks.Add(session.DisposeAsync().AsTask());
      foreach (var client in clients)
         disposeTasks.Add(client.DisposeAsync().AsTask());

      await Task.WhenAll(disposeTasks);
      await listener.UnbindAsync(reportingCts.Token);
      await listener.DisposeAsync();

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.WriteLine("                     BENCHMARK RUN COMPLETED                      ");
      Console.WriteLine($"Total Pings Received by Server: {_serverPingsReceived:N0}");
      Console.WriteLine($"Total Pongs Received by Clients: {_clientPongsReceived:N0}");
      Console.WriteLine("==================================================================");
      Console.ResetColor();
   }

   private static async Task HandleServerSessionAsync(INetworkSession session, CancellationToken ct)
   {
      try
      {
         var streamResult = await session.AcceptStreamAsync(ct);
         if (streamResult.Failed) return;

         await using var stream = streamResult.Success!;
         var reader = stream.Transport.Input;
         var writer = stream.Transport.Output;

         while (!ct.IsCancellationRequested)
         {
            var readResult = await reader.ReadAsync(ct);
            var buffer = readResult.Buffer;

            // Wait until we have at least a full packet (4 bytes)
            while (buffer.Length < 4 && !readResult.IsCompleted && !readResult.IsCanceled)
            {
               reader.AdvanceTo(buffer.Start, buffer.End);
               readResult = await reader.ReadAsync(ct);
               buffer = readResult.Buffer;
            }

            if (buffer.Length >= 4)
            {
               var span = buffer.Slice(0, 4);
               if (span.FirstSpan.SequenceEqual("PING"u8))
               {
                  Interlocked.Increment(ref _serverPingsReceived);

                  // Respond with PONG
                  var memory = writer.GetMemory(4);
                  "PONG"u8.CopyTo(memory.Span);
                  writer.Advance(4);
                  await writer.FlushAsync(ct);
               }

               reader.AdvanceTo(buffer.GetPosition(4));
            }
            else
            {
               reader.AdvanceTo(buffer.Start, buffer.End);
            }

            if (readResult.IsCompleted || readResult.IsCanceled) break;
         }
      }
      catch (Exception)
      {
         // Client closed connection
      }
      finally
      {
         await session.DisposeAsync();
      }
   }

   private static async Task RunClientPingPongLoopAsync(INetworkSession session, CancellationToken ct)
   {
      try
      {
         var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
         if (streamResult.Failed) return;

         await using var stream = streamResult.Success!;
         var reader = stream.Transport.Input;
         var writer = stream.Transport.Output;

         while (!ct.IsCancellationRequested)
         {
            // Write PING
            var memory = writer.GetMemory(4);
            "PING"u8.CopyTo(memory.Span);
            writer.Advance(4);
            await writer.FlushAsync(ct);

            // Read PONG
            var readResult = await reader.ReadAsync(ct);
            var buffer = readResult.Buffer;

            while (buffer.Length < 4 && !readResult.IsCompleted && !readResult.IsCanceled)
            {
               reader.AdvanceTo(buffer.Start, buffer.End);
               readResult = await reader.ReadAsync(ct);
               buffer = readResult.Buffer;
            }

            if (buffer.Length >= 4)
            {
               var span = buffer.Slice(0, 4);
               if (span.FirstSpan.SequenceEqual("PONG"u8))
               {
                  Interlocked.Increment(ref _clientPongsReceived);
               }
               reader.AdvanceTo(buffer.GetPosition(4));
            }
            else
            {
               reader.AdvanceTo(buffer.Start, buffer.End);
            }

            if (readResult.IsCompleted || readResult.IsCanceled) break;

            // Pacing delay (1 second) to simulate continuous keep-alive load without spinning CPU
            await Task.Delay(1000, ct);
         }
      }
      catch (Exception)
      {
         // Closed or aborted
      }
   }
}
