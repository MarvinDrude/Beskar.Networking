using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Networking.Benchmarks.Common;

public record BenchmarkConfig(
   int ClientCount,
   int PayloadSize,
   int DurationSeconds,
   EndPoint EndPoint
);

public static class GenericThroughputBenchmarkRunner
{
   public static async Task RunAsync(
      INetworkListener listener,
      Func<INetworkClient> clientFactory,
      BenchmarkConfig config,
      string transportName)
   {
      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.WriteLine($"               BESKAR {transportName.ToUpper()} THROUGHPUT BENCHMARK              ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine("--- Starting Benchmark Setup ---");
      Console.WriteLine($"EndPoint:     {config.EndPoint}");
      Console.WriteLine($"Clients:      {config.ClientCount}");
      Console.WriteLine($"Payload Size: {config.PayloadSize} bytes");
      Console.WriteLine($"Duration:     {config.DurationSeconds} seconds");
      Console.ResetColor();
      Console.WriteLine();

      var payload = new byte[config.PayloadSize];
      RandomNumberGenerator.Fill(payload);

      var bindResult = await listener.BindAsync();
      if (bindResult.Failed)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"Error starting {transportName} Listener: {bindResult.Error.Message}");
         Console.ResetColor();
         return;
      }

      using var cts = new CancellationTokenSource();
      var token = cts.Token;

      long totalSentBytes = 0;
      long totalReceivedBytes = 0;
      long totalSentPackets = 0;
      long totalReceivedPackets = 0;

      // Start server accept loop
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
                     if (!session.IsSupportingMultiplexing)
                     {
                        // For non-multiplexed transports like TCP/WS, there's only one stream.
                        // Run its read loop directly in the session task to keep the session alive.
                        var streamResult = await session.AcceptStreamAsync(token);
                        if (!streamResult.Failed)
                        {
                           var stream = streamResult.Success!;
                           try
                           {
                              var input = stream.Transport.Input;
                              var leftoverBytes = 0;

                              while (!token.IsCancellationRequested)
                              {
                                 var readResult = await input.ReadAsync(token);
                                 if (readResult.IsCompleted || readResult.IsCanceled) break;

                                 var buffer = readResult.Buffer;
                                 var length = buffer.Length;

                                 Interlocked.Add(ref totalReceivedBytes, length);

                                 var totalBytesToProcess = length + leftoverBytes;
                                 var packets = (int)(totalBytesToProcess / config.PayloadSize);
                                 leftoverBytes = (int)(totalBytesToProcess % config.PayloadSize);

                                 if (packets > 0) Interlocked.Add(ref totalReceivedPackets, packets);

                                 input.AdvanceTo(buffer.End);
                              }
                           }
                           catch
                           {
                              // ignore stream errors
                           }
                           finally
                           {
                              await stream.DisposeAsync();
                           }
                        }
                     }
                     else
                     {
                        // For multiplexed transports (QUIC), accept multiple concurrent streams.
                        var streamTasks = new List<Task>();
                        while (!token.IsCancellationRequested)
                        {
                           var streamResult = await session.AcceptStreamAsync(token);
                           if (streamResult.Failed) break;

                           var stream = streamResult.Success!;
                           streamTasks.Add(Task.Run(async () =>
                           {
                              try
                              {
                                 var input = stream.Transport.Input;
                                 var leftoverBytes = 0;
                                 while (!token.IsCancellationRequested)
                                 {
                                    var readResult = await input.ReadAsync(token);
                                    if (readResult.IsCompleted || readResult.IsCanceled) break;

                                    var buffer = readResult.Buffer;
                                    var length = buffer.Length;

                                    Interlocked.Add(ref totalReceivedBytes, length);

                                    var totalBytesToProcess = length + leftoverBytes;
                                    var packets = (int)(totalBytesToProcess / config.PayloadSize);
                                    leftoverBytes = (int)(totalBytesToProcess % config.PayloadSize);

                                    if (packets > 0) Interlocked.Add(ref totalReceivedPackets, packets);

                                    input.AdvanceTo(buffer.End);
                                 }
                              }
                              catch
                              {
                                 // ignore stream errors
                              }
                              finally
                              {
                                 await stream.DisposeAsync();
                              }
                           }, token));
                        }

                        await Task.WhenAll(streamTasks);
                     }
                  }
                  catch
                  {
                     // ignore session errors
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

      // Connect clients
      Console.WriteLine($"Connecting {config.ClientCount} {transportName} clients...");
      var clients = new INetworkClient[config.ClientCount];
      var clientSessions = new INetworkSession[config.ClientCount];
      var connectTasks = new Task[config.ClientCount];

      for (var i = 0; i < config.ClientCount; i++)
      {
         var clientId = i;
         clients[clientId] = clientFactory();
         connectTasks[clientId] = Task.Run(async () =>
         {
            var connectResult = await clients[clientId].ConnectAsync(config.EndPoint, token);
            if (connectResult.Failed)
               throw new InvalidOperationException(
                  $"Client {clientId} failed to connect: {connectResult.Error.Message}");
            clientSessions[clientId] = connectResult.Success!;
         });
      }

      try
      {
         await Task.WhenAll(connectTasks);
         Console.WriteLine($"All {transportName} clients connected.");
      }
      catch (Exception ex)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"Connection phase failed: {ex.Message}");
         Console.ResetColor();
         await Cleanup(listener, clients, clientSessions);
         return;
      }

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
      for (var i = 0; i < config.ClientCount; i++)
      {
         var session = clientSessions[i];
         clientWriteTasks.Add(Task.Run(async () =>
         {
            try
            {
               var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, token);
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
               // ignore write failures
            }
         }));
      }

      await Task.Delay(TimeSpan.FromSeconds(config.DurationSeconds));

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

   private static async Task Cleanup(INetworkListener listener, INetworkClient[] clients, INetworkSession[] sessions)
   {
      var disposeTasks = new List<Task>();
      foreach (var session in sessions)
         disposeTasks.Add(session.DisposeAsync().AsTask());

      foreach (var client in clients)
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
}
