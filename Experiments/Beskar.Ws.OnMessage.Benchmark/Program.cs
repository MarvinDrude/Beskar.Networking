using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Ws;

namespace Beskar.Ws.OnMessage.Benchmark;

public static class Program
{
   public static async Task Main(string[] args)
   {
      var clientCount = 20;
      var payloadSize = 512;
      var durationSeconds = 10;
      var serverPort = 9003;
      var mode = 2; // 1 = Round-Trip Echo (Ping-Pong), 2 = One-Way Streaming Sink

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.WriteLine("          BESKAR WS ONMESSAGE HIGH-LEVEL BENCHMARK               ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      clientCount = PromptInt("Number of clients", clientCount);
      payloadSize = PromptInt("Payload size (bytes)", payloadSize);
      durationSeconds = PromptInt("Test duration (seconds)", durationSeconds);
      serverPort = PromptInt("Server port", serverPort);
      mode = PromptInt("Benchmark Mode (1 = Round-Trip Echo, 2 = One-Way Streaming Sink)", mode);
      Console.WriteLine();

      var endPoint = new IPEndPoint(IPAddress.Loopback, serverPort);

      long totalSentBytes = 0;
      long totalReceivedBytes = 0;
      long totalSentPackets = 0;
      long totalReceivedPackets = 0;

      var isEchoMode = mode == 1;

      // Server options configured with High-Level OnMessage Handler & stats tracking
      var serverOptions = new WsTransportOptions
      {
         Path = "/benchmark",
         Subprotocol = "bench-protocol",
         KeepAliveInterval = TimeSpan.Zero,
         OnMessage = (session, payload, opcode) =>
         {
            Interlocked.Add(ref totalReceivedBytes, payload.Length);
            Interlocked.Increment(ref totalReceivedPackets);

            if (isEchoMode)
            {
               _ = session.SendFrameAsync(payload, opcode);
            }
         }
      };

      var clientOptions = new WsTransportOptions
      {
         Path = "/benchmark",
         Subprotocol = "bench-protocol",
         KeepAliveInterval = TimeSpan.Zero
      };

      var payload = new byte[payloadSize];
      RandomNumberGenerator.Fill(payload);

      var listener = new WsNetworkListener(endPoint, serverOptions);
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

      // Start accepting sessions on server
      var serverAcceptTask = Task.Run(async () =>
      {
         while (!token.IsCancellationRequested)
         {
            try
            {
               var acceptResult = await listener.AcceptSessionAsync(token);
               if (acceptResult.Failed) break;
            }
            catch
            {
               break;
            }
         }
      });

      // Connect clients
      Console.WriteLine($"Connecting {clientCount} WS (OnMessage) clients...");
      var clients = new WsNetworkClient[clientCount];
      var clientSessions = new INetworkSession[clientCount];
      var connectTasks = new Task[clientCount];

      for (var i = 0; i < clientCount; i++)
      {
         var clientId = i;
         clients[clientId] = new WsNetworkClient(clientOptions);
         connectTasks[clientId] = Task.Run(async () =>
         {
            var connectResult = await clients[clientId].ConnectAsync(endPoint, token);
            if (connectResult.Failed)
               throw new InvalidOperationException($"Client {clientId} failed to connect: {connectResult.Error.Message}");
            clientSessions[clientId] = connectResult.Success!;
         });
      }

      try
      {
         await Task.WhenAll(connectTasks);
         Console.WriteLine($"All WS (OnMessage) clients connected. Mode: {(isEchoMode ? "Round-Trip Echo" : "One-Way Streaming Sink")}");
      }
      catch (Exception ex)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"Connection phase failed: {ex.Message}");
         Console.ResetColor();
         return;
      }

      Console.ForegroundColor = ConsoleColor.Yellow;
      Console.WriteLine("==================================================================");
      Console.WriteLine("              RUNNING ONMESSAGE BENCHMARK TEST...                 ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      var stopwatch = Stopwatch.StartNew();

      // Client write tasks
      var clientTasks = new List<Task>();
      for (var i = 0; i < clientCount; i++)
      {
         var session = clientSessions[i];
         clientTasks.Add(Task.Run(async () =>
         {
            try
            {
               var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, token);
               if (streamResult.Failed) return;

               var stream = streamResult.Success!;
               var input = stream.Transport.Input;
               var output = stream.Transport.Output;

               if (isEchoMode)
               {
                  // Ping-pong round trip
                  while (!token.IsCancellationRequested)
                  {
                     await output.WriteAsync(payload, token);
                     var flushResult = await output.FlushAsync(token);
                     if (flushResult.IsCompleted || flushResult.IsCanceled) break;

                     Interlocked.Add(ref totalSentBytes, payload.Length);
                     Interlocked.Increment(ref totalSentPackets);

                     var readResult = await input.ReadAsync(token);
                     if (readResult.IsCompleted || readResult.IsCanceled) break;

                     input.AdvanceTo(readResult.Buffer.End);
                  }
               }
               else
               {
                  // One-way continuous streaming to server OnMessage
                  while (!token.IsCancellationRequested)
                  {
                     await output.WriteAsync(payload, token);
                     var flushResult = await output.FlushAsync(token);
                     if (flushResult.IsCompleted || flushResult.IsCanceled) break;

                     Interlocked.Add(ref totalSentBytes, payload.Length);
                     Interlocked.Increment(ref totalSentPackets);
                  }
               }
            }
            catch { }
         }));
      }

      // Reporter loop
      var reporterTask = Task.Run(async () =>
      {
         long prevSent = 0;
         long prevReceived = 0;
         var reportStopwatch = Stopwatch.StartNew();

         while (!token.IsCancellationRequested)
         {
            try { await Task.Delay(1000, token); }
            catch { break; }

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

            Console.WriteLine($"[{stopwatch.Elapsed:hh\\:mm\\:ss}] Client Sent: {sentMbRate:F2} MB/s | Server OnMessage Received: {receivedMbRate:F2} MB/s");
         }
      });

      await Task.Delay(TimeSpan.FromSeconds(durationSeconds));
      await cts.CancelAsync();

      try
      {
         await Task.WhenAll(clientTasks);
      }
      catch { }

      stopwatch.Stop();
      await reporterTask;

      var actualDuration = stopwatch.Elapsed.TotalSeconds;
      var finalSentBytes = Interlocked.Read(ref totalSentBytes);
      var finalReceivedBytes = Interlocked.Read(ref totalReceivedBytes);
      var finalSentPackets = Interlocked.Read(ref totalSentPackets);
      var finalReceivedPackets = Interlocked.Read(ref totalReceivedPackets);

      var sentMbRateFinal = (finalSentBytes / actualDuration) / (1024 * 1024);
      var receivedMbRateFinal = (finalReceivedBytes / actualDuration) / (1024 * 1024);
      var sentMsgRate = finalSentPackets / actualDuration;
      var receivedMsgRate = finalReceivedPackets / actualDuration;

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine();
      Console.WriteLine("==================================================================");
      Console.WriteLine("                ONMESSAGE FINAL STATS REPORT                      ");
      Console.WriteLine("==================================================================");
      Console.ForegroundColor = ConsoleColor.White;
      Console.WriteLine($"Mode:                          {(isEchoMode ? "Round-Trip Echo (Ping-Pong)" : "One-Way Streaming Sink")}");
      Console.WriteLine($"Actual Test Duration:          {actualDuration:F2} seconds");
      Console.WriteLine($"Client Frames Sent:            {finalSentPackets:N0}");
      Console.WriteLine($"Server OnMessage Recv:         {finalReceivedPackets:N0}");
      Console.WriteLine();
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"Client Throughput:             {sentMsgRate:F0} frames/s ({sentMbRateFinal:F2} MB/s)");
      Console.WriteLine($"Server OnMessage Throughput:  {receivedMsgRate:F0} frames/s ({receivedMbRateFinal:F2} MB/s)");
      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.ResetColor();
      Console.WriteLine();

      await listener.UnbindAsync();
      await listener.DisposeAsync();
   }

   private static int PromptInt(string prompt, int defaultValue)
   {
      Console.Write($"{prompt} [default: {defaultValue}]: ");
      var input = Console.ReadLine();
      if (string.IsNullOrWhiteSpace(input)) return defaultValue;
      return int.TryParse(input, out var value) ? value : defaultValue;
   }
}
