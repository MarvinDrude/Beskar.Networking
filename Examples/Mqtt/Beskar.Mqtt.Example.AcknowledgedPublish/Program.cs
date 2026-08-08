using System.Net;
using System.Text;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Builders.Unsubscribing;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server;
using Beskar.Utilities.Tracing;

// Disable raw trace logger to use clean, synchronized Console.WriteLine output
TraceLogger.IsEnabled = false;

ConsoleLogger.Write();
ConsoleLogger.Write("====================================================================");
ConsoleLogger.Write(" MQTT Publish & Subscriber Acknowledgment (Request-Response) Example");
ConsoleLogger.Write("====================================================================");

const int Port = 8011;
const string CommandTopic = "tasks/orders/process";
const string AckTopic = "tasks/orders/ack";

// 1. Build and start local MQTT Server
var mqttServer = MqttServerFactory.CreateBuilder()
   .WithDefaultClientIdGenerator()
   .UseTcp(Port)
   .Build();

ConsoleLogger.WriteColor(ConsoleColor.Cyan, "[Server] Starting local MQTT broker...");
var startResult = await mqttServer.StartAsync();
if (startResult.Failed)
{
   throw new InvalidOperationException($"Server failed to start: {startResult.Error.Detail}");
}
ConsoleLogger.WriteColor(ConsoleColor.Cyan, $"[Server] Running and listening on port {Port}.");

// 2. Initialize Publisher (Dispatcher) and Subscriber (Worker) clients
ConsoleLogger.Write("\n--- Initializing MQTT Clients ---");
await using var publisherClient = MqttClientFactory.CreateTcp();
await using var subscriberWorkerClient = MqttClientFactory.CreateTcp();

// Register handler on Subscriber (Worker) Client to process requests and return an Application ACK via context.RespondAsync
using var subReceiveHandlerToken = subscriberWorkerClient.AddMessageReceiveHandler(async (context, ct) =>
{
   var requestPayload = Encoding.UTF8.GetString(context.Message.Payload.Span);
   byte[]? correlationDataBytes = context.Message.CorrelationData?.ToArray();
   var correlationId = correlationDataBytes != null ? Encoding.UTF8.GetString(correlationDataBytes) : null;

   ConsoleLogger.WriteColor(
      ConsoleColor.Yellow,
      $"[Worker Subscriber] Received message on '{context.Message.Topic}' | CorrelationId: '{correlationId ?? "(none)"}' | Payload: {requestPayload}");

   // Process request and send application ACK back using context.RespondAsync(...)
   if (!string.IsNullOrEmpty(context.Message.ResponseTopic))
   {
      var isOrderValid = !requestPayload.Contains("\"invalid\"");
      var ackStatus = isOrderValid ? "ACKNOWLEDGED" : "REJECTED_INVALID_DATA";
      var ackMessage = $"{{ \"status\": \"{ackStatus}\", \"processedBy\": \"Worker-1\", \"originalPayload\": {requestPayload} }}";

      ConsoleLogger.WriteColor(
         ConsoleColor.Yellow,
         $"[Worker Subscriber] Sending reply via context.RespondAsync to topic '{context.Message.ResponseTopic}'...");

      var ackResult = await context.RespondAsync(ackMessage, ct: ct);
      if (ackResult.Failed)
      {
         ConsoleLogger.WriteColor(ConsoleColor.Red, $"[Worker Subscriber] Failed to send ACK: {ackResult.Error.Detail}");
      }
   }
});

// 3. Connect Clients to the Broker
ConsoleLogger.Write("\n--- Connecting Clients ---");
var connectOptions = new ConnectOptions
{
   EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
   ProtocolVersion = MqttProtocolVersion.V50
};

ConsoleLogger.Write("[Dispatcher Publisher] Connecting...");
await publisherClient.ConnectAsync(connectOptions);

ConsoleLogger.Write("[Worker Subscriber] Connecting...");
await subscriberWorkerClient.ConnectAsync(connectOptions);

// 4. Subscribe Subscriber to Command topic
ConsoleLogger.Write("\n--- Subscribing Topics ---");
var workerSubOptions = SubscribeOptions.Create()
   .WithTopicFilter(CommandTopic, QualityOfServiceType.AtLeastOnce)
   .Build();
await subscriberWorkerClient.SubscribeAsync(workerSubOptions);
ConsoleLogger.Write($"[Worker Subscriber] Subscribed to command topic '{CommandTopic}'");

// 5. Publish Tasks and Wait for Subscriber Acknowledgments using publisherClient.RequestAsync(...)
ConsoleLogger.Write("\n--- Publishing Tasks with RequestAsync Acknowledgment ---");

async Task PublishAndAwaitAckAsync(string taskId, string jsonPayload)
{
   var correlationId = $"req-{taskId}";
   var requestOptions = PublishOptions.Create()
      .WithTopic(CommandTopic)
      .WithPayload(jsonPayload)
      .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
      .WithResponseTopic(AckTopic)
      .WithCorrelationData(Encoding.UTF8.GetBytes(correlationId))
      .Build();

   ConsoleLogger.WriteColor(
      ConsoleColor.Blue,
      $"[Dispatcher Publisher] Sending request '{taskId}' via publisherClient.RequestAsync...");

   var responseResult = await publisherClient.RequestAsync(requestOptions, timeout: TimeSpan.FromSeconds(5));

   if (responseResult.Failed)
   {
      ConsoleLogger.WriteColor(ConsoleColor.Red, $"[Dispatcher Publisher] Request failed: {responseResult.Error.Detail}");
   }
   else
   {
      var response = responseResult.Success;
      var ackPayload = Encoding.UTF8.GetString(response.Payload.Span);
      ConsoleLogger.WriteColor(
         ConsoleColor.Green,
         $"[Dispatcher Publisher] SUCCESS! Received subscriber acknowledgment in {response.Elapsed.TotalMilliseconds:F1}ms for '{taskId}':\n   -> {ackPayload}\n");
   }
}

// Case 1: Valid Task
await PublishAndAwaitAckAsync("TASK-101", "{ \"orderId\": \"ORD-101\", \"amount\": 150.75 }");

// Case 2: Task requiring rejection acknowledgment
await PublishAndAwaitAckAsync("TASK-102", "{ \"orderId\": \"ORD-102\", \"status\": \"invalid\" }");

// 6. Clean up and Shutdown
ConsoleLogger.Write("--- Cleaning Up ---");
var unsubOptions = UnsubscribeOptions.Create()
   .WithTopicFilter(CommandTopic)
   .Build();

await subscriberWorkerClient.UnsubscribeAsync(unsubOptions);

await publisherClient.DisconnectAsync(new DisconnectOptions());
await subscriberWorkerClient.DisconnectAsync(new DisconnectOptions());

await mqttServer.StopAsync();

ConsoleLogger.Write("====================================================================");
ConsoleLogger.Write(" Acknowledged Publish Example Finished Successfully.");
ConsoleLogger.Write("====================================================================");

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
