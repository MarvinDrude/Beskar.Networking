using System.Collections.Concurrent;
using System.Diagnostics;
using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using Beskar.Utilities.Tracing;

// Disable raw trace logger to use clean, synchronized Console output
TraceLogger.IsEnabled = false;

ConsoleLogger.Write();
ConsoleLogger.Write("========================================================================");
ConsoleLogger.Write(" Beskar MQTT High-Scale Job Status Updates Experiment                   ");
ConsoleLogger.Write("========================================================================");

const int Port = 8025;
const int WorkerCount = 10;
const int ListenerCount = 10;
const int JobsPerWorker = 50;
const int UpdatesPerJob = 10;
const int TotalJobs = WorkerCount * JobsPerWorker; // 500 unique job topics
const int TotalUpdates = TotalJobs * UpdatesPerJob; // 5,000 status update publishes

ConsoleLogger.Write($"Configuration:");
ConsoleLogger.Write($"  - Workers:              {WorkerCount}");
ConsoleLogger.Write($"  - Listeners:            {ListenerCount}");
ConsoleLogger.Write($"  - Total Unique Jobs:    {TotalJobs} (Topics: 'jobs/status/JOB-xxxx')");
ConsoleLogger.Write($"  - Updates Per Job:      {UpdatesPerJob}");
ConsoleLogger.Write($"  - Total Updates Sent:   {TotalUpdates:N0}");

// 1. Start local MQTT Broker
var mqttServer = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(Port)
   .Build();

ConsoleLogger.WriteColor(ConsoleColor.Cyan, "\n[Server] Starting MQTT broker...");
var startResult = await mqttServer.StartAsync();
if (startResult.Failed)
{
   throw new InvalidOperationException($"Broker failed to start: {startResult.Error.Detail}");
}
ConsoleLogger.WriteColor(ConsoleColor.Cyan, $"[Server] Running on port {Port}.");

var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
   ProtocolVersion = MqttProtocolVersion.V50
};

// 2. Initialize Listener Clients and subscribe each to its subset of jobs
ConsoleLogger.WriteColor(ConsoleColor.Yellow, "\n[Setup] Initializing & Connecting Listener Clients...");
var listeners = new IMqttClient[ListenerCount];
var targetedReceivedCount = 0L;
var totalLatencyTicks = 0L;

for (var i = 0; i < ListenerCount; i++)
{
   var listenerId = i;
   listeners[i] = MqttClientFactory.CreateTcp();
   await listeners[i].ConnectAsync(connectOptions);

   listeners[i].AddMessageReceiveHandler((ctx, ct) =>
   {
      Interlocked.Increment(ref targetedReceivedCount);

      if (ctx.Message.CorrelationData.HasValue && ctx.Message.CorrelationData.Value.Length >= 8)
      {
         var sendTimestamp = BitConverter.ToInt64(ctx.Message.CorrelationData.Value.Span);
         var elapsedTicks = Stopwatch.GetTimestamp() - sendTimestamp;
         Interlocked.Add(ref totalLatencyTicks, elapsedTicks);
      }

      return ValueTask.CompletedTask;
   });
}

// Subscribe listeners to their assigned dynamic topics (jobs/status/JOB-xxxx)
ConsoleLogger.WriteColor(ConsoleColor.Yellow, "[Setup] Subscribing Listeners to specific dynamic job topics...");
for (var jobId = 0; jobId < TotalJobs; jobId++)
{
   var assignedListenerIndex = jobId % ListenerCount;
   var topic = $"jobs/status/JOB-{jobId:D4}";

   var subOptions = SubscribeOptions.Create()
      .WithTopicFilter(topic, QualityOfServiceType.AtLeastOnce)
      .Build();

   await listeners[assignedListenerIndex].SubscribeAsync(subOptions);
}

// 3. Initialize Global Monitor Client subscribing to wildcard 'jobs/status/+'
ConsoleLogger.WriteColor(ConsoleColor.Yellow, "[Setup] Connecting Global Wildcard Monitor Client ('jobs/status/+')...");
await using var globalMonitorClient = MqttClientFactory.CreateTcp();
await globalMonitorClient.ConnectAsync(connectOptions);

var globalReceivedCount = 0L;
globalMonitorClient.AddMessageReceiveHandler((ctx, ct) =>
{
   Interlocked.Increment(ref globalReceivedCount);
   return ValueTask.CompletedTask;
});

var monitorSubOptions = SubscribeOptions.Create()
   .WithTopicFilter("jobs/status/+", QualityOfServiceType.AtLeastOnce)
   .Build();
await globalMonitorClient.SubscribeAsync(monitorSubOptions);

// 4. Initialize Worker Clients
ConsoleLogger.WriteColor(ConsoleColor.Green, "\n[Setup] Initializing & Connecting Background Worker Clients...");
var workers = new IMqttClient[WorkerCount];
for (var i = 0; i < WorkerCount; i++)
{
   workers[i] = MqttClientFactory.CreateTcp();
   await workers[i].ConnectAsync(connectOptions);
}

// 5. Execute High-Scale Job Status Updates Experiment
ConsoleLogger.WriteColor(ConsoleColor.Magenta, "\n========================================================================");
ConsoleLogger.WriteColor(ConsoleColor.Magenta, " STARTING EXPERIMENT: Pushing Job Status Updates across Workers...");
ConsoleLogger.WriteColor(ConsoleColor.Magenta, "========================================================================");

var publishedCount = 0L;
var sw = Stopwatch.StartNew();

var workerTasks = new Task[WorkerCount];
for (var w = 0; w < WorkerCount; w++)
{
   var workerIndex = w;
   var workerClient = workers[w];

   workerTasks[w] = Task.Run(async () =>
   {
      var startJobId = workerIndex * JobsPerWorker;
      var endJobId = startJobId + JobsPerWorker;

      for (var u = 1; u <= UpdatesPerJob; u++)
      {
         for (var jobId = startJobId; jobId < endJobId; jobId++)
         {
            var topic = $"jobs/status/JOB-{jobId:D4}";
            var payloadStr = $"{{\"jobId\":\"JOB-{jobId:D4}\",\"update\":{u},\"status\":\"PROCESSING\",\"progress\":{u * 10}}}";
            var timestampBytes = BitConverter.GetBytes(Stopwatch.GetTimestamp());

            var pubOptions = PublishOptions.Create()
               .WithTopic(topic)
               .WithPayload(payloadStr)
               .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
               .WithCorrelationData(timestampBytes)
               .Build();

            var pubResult = await workerClient.PublishAsync(pubOptions);
            if (!pubResult.Failed)
            {
               Interlocked.Increment(ref publishedCount);
            }
         }
      }
   });
}

await Task.WhenAll(workerTasks);
sw.Stop();

// Wait briefly for all in-flight messages to reach targeted subscribers and monitor
await Task.Delay(500);

var elapsedSec = sw.Elapsed.TotalSeconds;
var pubThroughput = publishedCount / elapsedSec;
var targetedThroughput = Volatile.Read(ref targetedReceivedCount) / elapsedSec;
var globalThroughput = Volatile.Read(ref globalReceivedCount) / elapsedSec;

var avgLatencyMs = 0.0;
var totalRec = Volatile.Read(ref targetedReceivedCount);
if (totalRec > 0)
{
   var avgTicks = (double)Volatile.Read(ref totalLatencyTicks) / totalRec;
   avgLatencyMs = (avgTicks / Stopwatch.Frequency) * 1000.0;
}

// 6. Print Benchmark Results
ConsoleLogger.WriteColor(ConsoleColor.Green, "\n========================================================================");
ConsoleLogger.WriteColor(ConsoleColor.Green, " EXPERIMENT RESULTS SUMMARY");
ConsoleLogger.WriteColor(ConsoleColor.Green, "========================================================================");
ConsoleLogger.Write($"  - Elapsed Time:                    {elapsedSec:F3} seconds");
ConsoleLogger.Write($"  - Total Status Updates Published:   {publishedCount:N0} msgs");
ConsoleLogger.Write($"  - Publish Throughput:              {pubThroughput:N1} msgs/sec");
ConsoleLogger.Write($"  - Targeted Listeners Received:      {targetedReceivedCount:N0} msgs ({targetedThroughput:N1} msgs/sec)");
ConsoleLogger.Write($"  - Wildcard Monitor Received:        {globalReceivedCount:N0} msgs ({globalThroughput:N1} msgs/sec)");
ConsoleLogger.Write($"  - Average Round-Trip Delivery RTT: {avgLatencyMs:F3} ms");
ConsoleLogger.WriteColor(ConsoleColor.Green, "========================================================================");

// 7. Cleanup
ConsoleLogger.Write("\n--- Cleaning Up Clients and Broker ---");
for (var i = 0; i < WorkerCount; i++)
{
   await workers[i].DisconnectAsync(new Beskar.Mqtt.Common.Builders.Disconnecting.DisconnectOptions());
   await workers[i].DisposeAsync();
}

for (var i = 0; i < ListenerCount; i++)
{
   await listeners[i].DisconnectAsync(new Beskar.Mqtt.Common.Builders.Disconnecting.DisconnectOptions());
   await listeners[i].DisposeAsync();
}

await mqttServer.StopAsync();

ConsoleLogger.Write("========================================================================");
ConsoleLogger.Write(" Job Updates Experiment Finished Successfully.");
ConsoleLogger.Write("========================================================================");

/// <summary>
/// Thread-safe Console logger utilizing Console.WriteLine protected by a lock object.
/// </summary>
file static class ConsoleLogger
{
   private static readonly Lock ConsoleLock = new();

   public static void Write(string message = "")
   {
      lock (ConsoleLock)
      {
         Console.WriteLine(message);
      }
   }

   public static void WriteColor(ConsoleColor color, string message)
   {
      lock (ConsoleLock)
      {
         var original = Console.ForegroundColor;
         Console.ForegroundColor = color;
         Console.WriteLine(message);
         Console.ForegroundColor = original;
      }
   }
}
