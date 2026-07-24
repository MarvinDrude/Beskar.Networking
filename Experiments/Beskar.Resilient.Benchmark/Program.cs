using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Server;

namespace Beskar.Resilient.Benchmark;

public static class Program
{
   public static async Task Main(string[] args)
   {
      // ==========================================
      // DEFAULT BENCHMARK CONFIGURATION
      // ==========================================
      var mode = 1; // 1 = Ingress, 2 = Echo, 3 = Broadcast
      var clientCount = 20; // Total number of resilient clients
      var payloadSize = 512; // Size of the message payload in bytes
      var durationSeconds = 10; // Duration of the benchmark test in seconds
      var publishConcurrency = 3; // Number of concurrent sending loops per client
      var serverPort = 5000; // Local port for the resilient server to listen on
      // ==========================================

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.WriteLine("               BESKAR RESILIENT THROUGHPUT BENCHMARK              ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      // Allow interactive configuration overrides
      Console.WriteLine("Press ENTER to use defaults, or customize the parameters below:");
      Console.WriteLine();

      Console.WriteLine("Benchmark Modes:");
      Console.WriteLine("  1. Ingress   (Client -> Server only)");
      Console.WriteLine("  2. Echo      (Client -> Server -> Client echo)");
      Console.WriteLine("  3. Broadcast (Client -> Server -> All Clients broadcast)");
      mode = PromptInt("Benchmark Mode", mode);
      clientCount = PromptInt("Number of clients", clientCount);
      payloadSize = PromptInt("Payload size (bytes)", payloadSize);
      durationSeconds = PromptInt("Test duration (seconds)", durationSeconds);
      publishConcurrency = PromptInt("Send concurrency per client", publishConcurrency);
      serverPort = PromptInt("Server port", serverPort);

      Console.WriteLine();
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine("--- Starting Benchmark Setup ---");
      Console.WriteLine($"Benchmark Mode:      {(mode == 1 ? "Ingress" : mode == 2 ? "Echo" : "Broadcast")}");
      Console.WriteLine($"Server Port:         {serverPort}");
      Console.WriteLine($"Clients:             {clientCount}");
      Console.WriteLine($"Payload Size:        {payloadSize} bytes");
      Console.WriteLine($"Duration:            {durationSeconds} seconds");
      Console.WriteLine($"Send Concurrency:    {publishConcurrency} task(s)/client");
      Console.ResetColor();
      Console.WriteLine();

      var payload = new byte[payloadSize];
      RandomNumberGenerator.Fill(payload);

      var msgFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Message, new ReadOnlySequence<byte>(payload));

      long totalSentMessages = 0;
      long totalReceivedMessagesOnServer = 0;
      long totalReceivedMessagesOnClients = 0;

      Console.WriteLine("Starting Resilient Server...");
      var serverOptions = new ResilientServerOptions
      {
         FrameReceivedAllPackets = true
      };
      
      var server = ResilientServerFactory.CreateBuilder(serverOptions)
         .UseTcp(new IPEndPoint(IPAddress.Loopback, serverPort))
         .Build();

      // Setup Server FrameReceived logic based on mode
      server.Events.FrameReceived.Add(async (ctx, _) =>
      {
         if (ctx.Frame.GetFrameKind() == ResilientFrameKind.Message)
         {
            Interlocked.Increment(ref totalReceivedMessagesOnServer);

            if (mode == 2) // Echo
            {
               try
               {
                  await ctx.Client.SendAsync(ctx.Frame);
               }
               catch
               {
                  // ignored
               }
            }
            else if (mode == 3) // Broadcast
            {
               var clients = server.Clients.GetAll();
               foreach (var c in clients)
               {
                  try
                  {
                     await c.SendAsync(ctx.Frame);
                  }
                  catch
                  {
                     // ignored
                  }
               }
            }
         }
      });

      var startResult = await server.StartAsync();
      if (startResult.Failed)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"Error starting Resilient Server: {startResult.Error.Detail}");
         Console.ResetColor();
         return;
      }

      Console.WriteLine("Resilient Server started successfully.");

      Console.WriteLine($"Initializing and connecting {clientCount} clients...");
      var clients = new ResilientClient<BeskarPacket>[clientCount];
      var connectTasks = new Task[clientCount];

      for (var i = 0; i < clientCount; i++)
      {
         var clientId = i;
         var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: new ResilientClientOptions
         {
            Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = false }
         });
         clients[clientId] = client;

         if (mode is 2 or 3) // Echo or Broadcast
         {
            client.Events.FrameReceived.Add((ctx, _) =>
            {
               if (ctx.Frame.GetFrameKind() == ResilientFrameKind.Message)
               {
                  Interlocked.Increment(ref totalReceivedMessagesOnClients);
               }
               return ValueTask.CompletedTask;
            });
         }

         connectTasks[clientId] = Task.Run(async () =>
         {
            var result = await client.ConnectAsync(new IPEndPoint(IPAddress.Loopback, serverPort));
            if (result.Failed)
               throw new InvalidOperationException($"Client {clientId} failed to connect: {result.Error.Detail}");
         });
      }

      try
      {
         await Task.WhenAll(connectTasks);
         Console.WriteLine("All clients connected successfully.");
      }
      catch (Exception ex)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"Connection phase failed: {ex.Message}");
         Console.ResetColor();
         await Cleanup(server, clients);
         return;
      }

      Console.ForegroundColor = ConsoleColor.Yellow;
      Console.WriteLine();
      Console.WriteLine("==================================================================");
      Console.WriteLine("                    RUNNING BENCHMARK TEST...                     ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      using var cts = new CancellationTokenSource();
      var stopwatch = Stopwatch.StartNew();

      var reporterTask = Task.Run(async () =>
      {
         long prevSent = 0;
         long prevServerRecv = 0;
         long prevClientRecv = 0;
         var reportStopwatch = Stopwatch.StartNew();

         while (!cts.Token.IsCancellationRequested)
         {
            try
            {
               await Task.Delay(1000, cts.Token);
            }
            catch (OperationCanceledException)
            {
               break;
            }

            var elapsedSeconds = reportStopwatch.Elapsed.TotalSeconds;
            reportStopwatch.Restart();

            var currentSent = Interlocked.Read(ref totalSentMessages);
            var currentServerRecv = Interlocked.Read(ref totalReceivedMessagesOnServer);
            var currentClientRecv = Interlocked.Read(ref totalReceivedMessagesOnClients);

            var diffSent = currentSent - prevSent;
            var diffServerRecv = currentServerRecv - prevServerRecv;
            var diffClientRecv = currentClientRecv - prevClientRecv;

            prevSent = currentSent;
            prevServerRecv = currentServerRecv;
            prevClientRecv = currentClientRecv;

            var sentRate = diffSent / elapsedSeconds;
            var serverRecvRate = diffServerRecv / elapsedSeconds;
            var clientRecvRate = diffClientRecv / elapsedSeconds;

            if (mode == 1)
            {
               Console.WriteLine(
                  $"[{stopwatch.Elapsed:hh\\:mm\\:ss}] Sent: {sentRate:F0} msg/s | Received (Server): {serverRecvRate:F0} msg/s");
            }
            else
            {
               Console.WriteLine(
                  $"[{stopwatch.Elapsed:hh\\:mm\\:ss}] Sent: {sentRate:F0} msg/s | Server Recv: {serverRecvRate:F0} msg/s | Client Recv: {clientRecvRate:F0} msg/s");
            }
         }
      });

      // Start publishing tasks
      var publishTasks = new List<Task>();
      for (var i = 0; i < clientCount; i++)
      {
         var client = clients[i];
         for (var c = 0; c < publishConcurrency; c++)
            publishTasks.Add(Task.Run(async () =>
            {
               var token = cts.Token;

               while (!token.IsCancellationRequested)
               {
                  try
                  {
                     await client.SendAsync(msgFrame, token);
                     Interlocked.Increment(ref totalSentMessages);
                  }
                  catch (OperationCanceledException)
                  {
                     break;
                  }
                  catch (Exception)
                  {
                     // ignore failed publishes during high load
                  }
               }
            }));
      }

      // Run test for configured duration
      await Task.Delay(TimeSpan.FromSeconds(durationSeconds));

      // Stopping & Final Statistics
      Console.WriteLine();
      Console.WriteLine("Stopping sending tasks...");
      await cts.CancelAsync();

      try
      {
         await Task.WhenAll(publishTasks);
      }
      catch (Exception)
      {
         // Ignored cancellation exceptions
      }

      stopwatch.Stop();
      await reporterTask;

      // Allow a brief moment for final in-flight messages to arrive
      Console.WriteLine("Waiting 500ms for final in-flight messages to drain...");
      await Task.Delay(500);

      var finalSent = Interlocked.Read(ref totalSentMessages);
      var finalServerRecv = Interlocked.Read(ref totalReceivedMessagesOnServer);
      var finalClientRecv = Interlocked.Read(ref totalReceivedMessagesOnClients);

      var actualDuration = stopwatch.Elapsed.TotalSeconds;
      var avgSentRate = finalSent / actualDuration;
      var avgServerRecvRate = finalServerRecv / actualDuration;
      var avgClientRecvRate = finalClientRecv / actualDuration;

      var sentMbRate = avgSentRate * payloadSize / (1024 * 1024);
      var serverRecvMbRate = avgServerRecvRate * payloadSize / (1024 * 1024);
      var clientRecvMbRate = avgClientRecvRate * payloadSize / (1024 * 1024);

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine();
      Console.WriteLine("==================================================================");
      Console.WriteLine("                        FINAL STATS REPORT                        ");
      Console.WriteLine("==================================================================");
      Console.ForegroundColor = ConsoleColor.White;
      Console.WriteLine($"Actual Test Duration:    {actualDuration:F2} seconds");
      Console.WriteLine($"Total Messages Sent:     {finalSent:N0}");
      Console.WriteLine($"Total Messages Server:   {finalServerRecv:N0}");
      if (mode is 2 or 3)
      {
         Console.WriteLine($"Total Messages Clients:  {finalClientRecv:N0}");
      }
      Console.WriteLine();
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"Average Sent Throughput: {avgSentRate:F2} msg/s ({sentMbRate:F2} MB/s)");
      Console.WriteLine($"Average Server Recv:     {avgServerRecvRate:F2} msg/s ({serverRecvMbRate:F2} MB/s)");
      if (mode is 2 or 3)
      {
         Console.WriteLine($"Average Client Recv:     {avgClientRecvRate:F2} msg/s ({clientRecvMbRate:F2} MB/s)");
      }
      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.ResetColor();
      Console.WriteLine();

      // Teardown
      Console.WriteLine("Cleaning up resources...");
      await Cleanup(server, clients);
      Console.WriteLine("Benchmark completed.");
   }

   private static async Task Cleanup(ResilientServer<BeskarPacket> server, ResilientClient<BeskarPacket>[] clients)
   {
      var disconnectTasks = new List<Task>();
      foreach (var client in clients)
         if (client != null)
            disconnectTasks.Add(Task.Run(async () =>
            {
               try
               {
                  if (client.IsConnected) await client.DisconnectAsync();
                  await client.DisposeAsync();
               }
               catch
               {
                  // Ignored
               }
            }));

      await Task.WhenAll(disconnectTasks);

      try
      {
         await server.StopAsync();
         await server.DisposeAsync();
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
