using System.Diagnostics;
using System.Net;
using System.Security.Cryptography;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;

namespace Beskar.Mqtt.Benchmark;

public static class Program
{
   public static async Task Main(string[] args)
   {
      // ==========================================
      // DEFAULT BENCHMARK CONFIGURATION
      // ==========================================
      var clientCount = 20; // Total number of MQTT clients
      var payloadSize = 512; // Size of the publish payload in bytes
      var durationSeconds = 10; // Duration of the benchmark test in seconds
      var publishConcurrency = 3; // Number of concurrent publishing loops per client
      var serverPort = 1883; // Local port for the MQTT server to listen on
      // ==========================================

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.WriteLine("                  BESKAR MQTT THROUGHPUT BENCHMARK                ");
      Console.WriteLine("==================================================================");
      Console.ResetColor();

      // Allow interactive configuration overrides
      Console.WriteLine("Press ENTER to use defaults, or customize the parameters below:");
      Console.WriteLine();

      clientCount = PromptInt("Number of clients", clientCount);
      payloadSize = PromptInt("Payload size (bytes)", payloadSize);
      durationSeconds = PromptInt("Test duration (seconds)", durationSeconds);
      publishConcurrency = PromptInt("Publish concurrency per client", publishConcurrency);
      serverPort = PromptInt("Server port", serverPort);

      Console.WriteLine();
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine("--- Starting Benchmark Setup ---");
      Console.WriteLine($"Server Port:         {serverPort}");
      Console.WriteLine($"Clients:             {clientCount}");
      Console.WriteLine($"Payload Size:        {payloadSize} bytes");
      Console.WriteLine($"Duration:            {durationSeconds} seconds");
      Console.WriteLine($"Publish Concurrency: {publishConcurrency} task(s)/client");
      Console.WriteLine("Total Topics:        1000");
      Console.ResetColor();
      Console.WriteLine();

      var topics = Generate1000Topics();

      var payload = new byte[payloadSize];
      RandomNumberGenerator.Fill(payload);

      var publishOptionsList = PrebuildPublishOptions(topics, payload);

      Console.WriteLine("Starting MQTT Server...");
      var server = MqttServerFactory.CreateBuilder()
         .UseTcp(serverPort)
         .WithDefaultClientIdGenerator()
         .Build();

      var startResult = await server.StartAsync();
      if (startResult.Failed)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"Error starting MQTT Server: {startResult.Error.Detail}");
         Console.ResetColor();
         return;
      }

      Console.WriteLine("MQTT Server started successfully.");

      Console.WriteLine($"Initializing and connecting {clientCount} clients...");
      var clients = new IMqttClient[clientCount];
      var connectTasks = new Task[clientCount];

      long totalSentMessages = 0;
      long totalReceivedMessages = 0;

      for (var i = 0; i < clientCount; i++)
      {
         var clientId = i;
         var client = MqttClientFactory.CreateTcp();
         clients[clientId] = client;

         client.AddMessageReceiveHandler((context, token) =>
         {
            Interlocked.Increment(ref totalReceivedMessages);
            return ValueTask.CompletedTask;
         });

         var connectOptions = new ConnectOptionsBuilder(new IPEndPoint(IPAddress.Loopback, serverPort))
            .WithCleanSession()
            .WithKeepAlivePeriod(60)
            .WithClientId($"benchmark-client-{clientId}")
            .Build();

         connectTasks[clientId] = Task.Run(async () =>
         {
            var result = await client.ConnectAsync(connectOptions);
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

      Console.WriteLine("Subscribing clients to topics...");
      var subscribeTasks = new Task[clientCount];
      for (var i = 0; i < clientCount; i++)
      {
         var clientId = i;
         var client = clients[clientId];
         var subBuilder = new SubscribeOptionsBuilder();

         for (var topicIdx = 0; topicIdx < topics.Length; topicIdx++)
            if (topicIdx % clientCount == clientId)
               subBuilder.WithTopicFilter(topics[topicIdx], QualityOfServiceType.AtMostOnce);

         subscribeTasks[clientId] = Task.Run(async () =>
         {
            var result = await client.SubscribeAsync(subBuilder.Build());
            if (result.Failed)
               throw new InvalidOperationException($"Client {clientId} failed to subscribe: {result.Error.Detail}");
         });
      }

      try
      {
         await Task.WhenAll(subscribeTasks);
         Console.WriteLine("All client subscriptions completed.");
      }
      catch (Exception ex)
      {
         Console.ForegroundColor = ConsoleColor.Red;
         Console.WriteLine($"Subscription phase failed: {ex.Message}");
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
         long prevReceived = 0;
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
            var currentReceived = Interlocked.Read(ref totalReceivedMessages);

            var diffSent = currentSent - prevSent;
            var diffReceived = currentReceived - prevReceived;

            prevSent = currentSent;
            prevReceived = currentReceived;

            var sentRate = diffSent / elapsedSeconds;
            var receivedRate = diffReceived / elapsedSeconds;

            Console.WriteLine(
               $"[{stopwatch.Elapsed:hh\\:mm\\:ss}] Sent: {sentRate:F0} msg/s | Received: {receivedRate:F0} msg/s");
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
               var random = new Random();
               var token = cts.Token;

               while (!token.IsCancellationRequested)
               {
                  var options = publishOptionsList[random.Next(publishOptionsList.Length)];
                  try
                  {
                     var pubResult = await client.PublishAsync(options, token);
                     if (!pubResult.Failed) Interlocked.Increment(ref totalSentMessages);
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

      // 8. Stopping & Final Statistics
      Console.WriteLine();
      Console.WriteLine("Stopping publishing tasks...");
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
      var finalReceived = Interlocked.Read(ref totalReceivedMessages);

      var actualDuration = stopwatch.Elapsed.TotalSeconds;
      var avgSentRate = finalSent / actualDuration;
      var avgReceivedRate = finalReceived / actualDuration;

      var sentMbRate = avgSentRate * payloadSize / (1024 * 1024);
      var receivedMbRate = avgReceivedRate * payloadSize / (1024 * 1024);

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine();
      Console.WriteLine("==================================================================");
      Console.WriteLine("                        FINAL STATS REPORT                        ");
      Console.WriteLine("==================================================================");
      Console.ForegroundColor = ConsoleColor.White;
      Console.WriteLine($"Actual Test Duration:    {actualDuration:F2} seconds");
      Console.WriteLine($"Total Messages Sent:     {finalSent:N0}");
      Console.WriteLine($"Total Messages Received: {finalReceived:N0}");
      Console.WriteLine();
      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"Average Sent Throughput: {avgSentRate:F2} msg/s ({sentMbRate:F2} MB/s)");
      Console.WriteLine($"Average Recv Throughput: {avgReceivedRate:F2} msg/s ({receivedMbRate:F2} MB/s)");
      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("==================================================================");
      Console.ResetColor();
      Console.WriteLine();

      // 9. Teardown
      Console.WriteLine("Cleaning up resources...");
      await Cleanup(server, clients);
      Console.WriteLine("Benchmark completed.");
   }

   private static string[] Generate1000Topics()
   {
      var topics = new string[1000];
      string[] categories = ["sensor/temp", "sensor/humidity", "device/status", "home/lights", "industrial/telemetry"];

      var topicIdx = 0;
      foreach (var category in categories)
         for (var i = 0; i < 200; i++)
            topics[topicIdx++] = $"benchmark/{category}/{i}";

      return topics;
   }

   private static PublishOptions[] PrebuildPublishOptions(string[] topics, byte[] payload)
   {
      var optionsList = new PublishOptions[topics.Length];
      for (var i = 0; i < topics.Length; i++)
         optionsList[i] = new PublishOptionsBuilder()
            .WithTopic(topics[i])
            .WithPayload(payload)
            .WithQualityOfService(QualityOfServiceType.AtMostOnce)
            .Build();
      return optionsList;
   }

   private static async Task Cleanup(MqttServer server, IMqttClient[] clients)
   {
      var disconnectTasks = new List<Task>();
      foreach (var client in clients)
         if (client != null)
            disconnectTasks.Add(Task.Run(async () =>
            {
               try
               {
                  if (client.IsConnected) await client.DisconnectAsync(new DisconnectOptions());
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
