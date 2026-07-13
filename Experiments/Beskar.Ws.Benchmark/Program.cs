using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Ws;

namespace Beskar.Ws.Benchmark;

public static class Program
{
   public static async Task Main(string[] args)
   {
      // ==========================================
      // DEFAULT BENCHMARK CONFIGURATION
      // ==========================================
      var clientCount = 20; // Total number of raw WS connections
      var payloadSize = 1024; // Size of the WebSocket message in bytes
      var durationSeconds = 10; // Duration of the benchmark test in seconds
      var serverPort = 9002; // Local port for the WS server to listen on
      // ==========================================

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.WriteLine("                  BESKAR WS THROUGHPUT BENCHMARK                 ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      Console.WriteLine("Press ENTER to use defaults, or customize the parameters below:");
      Console.WriteLine();

      clientCount = PromptInt("Number of clients", clientCount);
      payloadSize = PromptInt("Payload size (bytes)", payloadSize);
      durationSeconds = PromptInt("Test duration (seconds)", durationSeconds);
      serverPort = PromptInt("Server port", serverPort);

      Console.WriteLine();
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine("--- Starting Benchmark Setup ---");
      Console.WriteLine($"Server Port:  {serverPort}");
      Console.WriteLine($"Clients:      {clientCount}");
      Console.WriteLine($"Payload Size: {payloadSize} bytes");
      Console.WriteLine($"Duration:     {durationSeconds} seconds");
      Console.ResetColor();
      Console.WriteLine();

      var payload = new byte[payloadSize];
      RandomNumberGenerator.Fill(payload);

      var endPoint = new IPEndPoint(IPAddress.Loopback, serverPort);
      var options = new WsTransportOptions
      {
         Path = "/benchmark",
         Subprotocol = "bench-protocol"
      };

      // Start WS Listener
      Console.WriteLine("Starting WS Listener...");
      var listener = new WsNetworkListener(endPoint, options);
      var bindResult = await listener.BindAsync();
      if (bindResult.Failed)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"Error starting WS Listener: {bindResult.Error.Message}");
         Console.ResetColor();
         return;
      }

      using var cts = new CancellationTokenSource();
      var token = cts.Token;

      long totalSentBytes = 0;
      long totalReceivedBytes = 0;
      long totalSentPackets = 0;
      long totalReceivedPackets = 0;

      // Start accepting task on the server
      var serverAcceptTask = Task.Run(async () =>
      {
         var activeServerTasks = new List<Task>();
         while (!token.IsCancellationRequested)
            try
            {
               var acceptResult = await listener.AcceptSessionAsync(token);
               if (acceptResult.Failed) break;

               var session = acceptResult.Success!;
               activeServerTasks.Add(Task.Run(async () =>
               {
                  try
                  {
                     var streamResult = await session.AcceptStreamAsync(token);
                     if (streamResult.Failed) return;

                     var stream = streamResult.Success!;
                     var input = stream.Transport.Input;

                     while (!token.IsCancellationRequested)
                     {
                        var readResult = await input.ReadAsync(token);
                        if (readResult.IsCompleted || readResult.IsCanceled) break;

                        var buffer = readResult.Buffer;
                        var length = buffer.Length;

                        Interlocked.Add(ref totalReceivedBytes, length);
                        Interlocked.Increment(ref totalReceivedPackets);

                        input.AdvanceTo(buffer.End);
                     }
                  }
                  catch
                  {
                     // ignore stream disconnects
                  }
                  finally
                  {
                     await session.DisposeAsync();
                  }
               }));
            }
            catch (OperationCanceledException)
            {
               break;
            }
            catch
            {
               // ignore accept errors
            }

         await Task.WhenAll(activeServerTasks);
      });

      // Initialize and Connect Clients
      Console.WriteLine($"Connecting {clientCount} WS clients...");
      var clients = new WsNetworkClient[clientCount];
      var clientSessions = new INetworkSession[clientCount];
      var connectTasks = new Task[clientCount];

      for (var i = 0; i < clientCount; i++)
      {
         var clientId = i;
         clients[clientId] = new WsNetworkClient(options);
         connectTasks[clientId] = Task.Run(async () =>
         {
            var connectResult = await clients[clientId].ConnectAsync(endPoint, token);
            if (connectResult.Failed)
               throw new InvalidOperationException(
                  $"Client {clientId} failed to connect: {connectResult.Error.Message}");
            clientSessions[clientId] = connectResult.Success!;
         });
      }

      try
      {
         await Task.WhenAll(connectTasks);
         Console.WriteLine("All WS clients connected.");
      }
      catch (Exception ex)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"Connection phase failed: {ex.Message}");
         Console.ResetColor();
         await Cleanup(listener, clients, clientSessions);
         return;
      }

      // Open Client Streams and Start Sending
      Console.ForegroundColor = ConsoleColor.Yellow;
      Console.WriteLine("==================================================================");
      Console.WriteLine("                    RUNNING BENCHMARK TEST...                     ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      var stopwatch = Stopwatch.StartNew();

      // Start reporter task
      var reporterTask = Task.Run(async () =>
      {
         long prevSent = 0;
         long prevReceived = 0;
         var reportStopwatch = Stopwatch.StartNew();

         while (!token.IsCancellationRequested)
         {
            try
            {
               await Task.Delay(1000, token);
            }
            catch (OperationCanceledException)
            {
               break;
            }

            var elapsedSeconds = reportStopwatch.Elapsed.TotalSeconds;
            reportStopwatch.Restart();

            var currentSent = Interlocked.Read(ref totalSentBytes);
            var currentReceived = Interlocked.Read(ref totalReceivedBytes);

            var diffSent = currentSent - prevSent;
            var diffReceived = currentReceived - prevReceived;

            prevSent = currentSent;
            prevReceived = currentReceived;

            var sentMbRate = diffSent / elapsedSeconds / (1024 * 1024);
            var receivedMbRate = diffReceived / elapsedSeconds / (1024 * 1024);

            Console.WriteLine(
               $"[{stopwatch.Elapsed:hh\\:mm\\:ss}] Sent: {sentMbRate:F2} MB/s | Received: {receivedMbRate:F2} MB/s");
         }
      });

      // Start client write tasks
      var clientWriteTasks = new List<Task>();
      for (var i = 0; i < clientCount; i++)
      {
         var session = clientSessions[i];
         clientWriteTasks.Add(Task.Run(async () =>
         {
            try
            {
               var streamResult = await session.AcceptStreamAsync(token);
               if (streamResult.Failed) return;

               var stream = streamResult.Success!;
               var output = stream.Transport.Output;

               while (!token.IsCancellationRequested)
               {
                  await output.WriteAsync(payload, token);
                  var flushResult = await output.FlushAsync(token);
                  if (flushResult.IsCompleted || flushResult.IsCanceled) break;

                  Interlocked.Add(ref totalSentBytes, payload.Length);
                  Interlocked.Increment(ref totalSentPackets);
               }
            }
            catch (OperationCanceledException)
            {
               // normal stop
            }
            catch
            {
               // ignore write failures on cancellation
            }
         }));
      }

      await Task.Delay(TimeSpan.FromSeconds(durationSeconds));

      Console.WriteLine();
      Console.WriteLine("Stopping benchmark tasks...");
      await cts.CancelAsync();

      try
      {
         await Task.WhenAll(clientWriteTasks);
         await serverAcceptTask;
      }
      catch (Exception)
      {
         // Ignored
      }

      stopwatch.Stop();
      await reporterTask;

      // Allow final bytes to drain
      await Task.Delay(500);

      var actualDuration = stopwatch.Elapsed.TotalSeconds;
      var finalSentBytes = Interlocked.Read(ref totalSentBytes);
      var finalReceivedBytes = Interlocked.Read(ref totalReceivedBytes);
      var finalSentPackets = Interlocked.Read(ref totalSentPackets);
      var finalReceivedPackets = Interlocked.Read(ref totalReceivedPackets);

      var avgSentRate = finalSentBytes / actualDuration;
      var avgReceivedRate = finalReceivedBytes / actualDuration;

      var sentMbRate = avgSentRate / (1024 * 1024);
      var receivedMbRate = avgReceivedRate / (1024 * 1024);

      var sentMsgRate = finalSentPackets / actualDuration;
      var receivedMsgRate = finalReceivedPackets / actualDuration;

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine();
      Console.WriteLine("==================================================================");
      Console.WriteLine("                        FINAL STATS REPORT                        ");
      Console.WriteLine("==================================================================");
      Console.ForegroundColor = ConsoleColor.White;
      Console.WriteLine($"Actual Test Duration:    {actualDuration:F2} seconds");
      Console.WriteLine($"Total Packets Sent:      {finalSentPackets:N0}");
      Console.WriteLine($"Total Packets Received:  {finalReceivedPackets:N0}");
      Console.WriteLine();
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"Average Sent Throughput: {sentMsgRate:F0} packets/s ({sentMbRate:F2} MB/s)");
      Console.WriteLine($"Average Recv Throughput: {receivedMsgRate:F0} packets/s ({receivedMbRate:F2} MB/s)");
      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.ResetColor();
      Console.WriteLine();

      Console.WriteLine("Cleaning up resources...");
      await Cleanup(listener, clients, clientSessions);
      Console.WriteLine("Benchmark completed.");
   }

   private static async Task Cleanup(WsNetworkListener listener, WsNetworkClient[] clients, INetworkSession[] sessions)
   {
      var disposeTasks = new List<Task>();
      foreach (var session in sessions)
         if (session != null)
            disposeTasks.Add(session.DisposeAsync().AsTask());

      foreach (var client in clients)
         if (client != null)
            disposeTasks.Add(client.DisposeAsync().AsTask());

      await Task.WhenAll(disposeTasks);

      try
      {
         await listener.UnbindAsync();
         await listener.DisposeAsync();
      }
      catch
      {
         // Ignored
      }
   }

   private static int PromptInt(string prompt, int defaultValue)
   {
      Console.Write($"{prompt} [default: {defaultValue}]: ");
      var input = Console.ReadLine();
      if (string.IsNullOrWhiteSpace(input)) return defaultValue;

      if (int.TryParse(input, out var value)) return value;

      Console.ForegroundColor = ConsoleColor.Red;
      Console.WriteLine($"Invalid input, using default value: {defaultValue}");
      Console.ResetColor();
      return defaultValue;
   }
}
