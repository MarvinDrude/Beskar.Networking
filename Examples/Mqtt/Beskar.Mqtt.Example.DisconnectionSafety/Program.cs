using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Text.Json;
using Beskar.Mqtt.Client;
using Beskar.Mqtt.Common.Interfaces;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Builders.Publishing;
using Beskar.Mqtt.Common.Builders.Subscribing;
using Beskar.Mqtt.Common.Options;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Models;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Server;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Mqtt.Server.Options;
using Beskar.Mqtt.Server.Enums;
using System.Buffers;

Console.WriteLine();
Console.WriteLine("==========================================================");
Console.WriteLine(" MQTT Disconnection Safety & Persistence Example          ");
Console.WriteLine("==========================================================");

const int Port = 8006;
const string RetainedMessagesFilePath = "retained_messages.json";
const string ClientOfflineQueueFilePath = "client_offline_queue.json";

// Cleanup old files from previous runs if they exist
if (File.Exists(RetainedMessagesFilePath)) File.Delete(RetainedMessagesFilePath);
if (File.Exists(ClientOfflineQueueFilePath)) File.Delete(ClientOfflineQueueFilePath);

try
{
   // =====================================================================
   // SECTION 1: Build MQTT Server with Persistent Sessions & JSON Retained Msg Store
   // =====================================================================
   TraceLogger.LogInfo("\n--- [Step 1] Starting MQTT Server with Persistence Enabled ---");

   var serverOptions = new MqttServerOptions
   {
      SupportPersistentSessions = true,
      MaxPendingMessagesPerConnection = 100,
      PendingMessageOverflowBehavior = MessageOverflowBehavior.DropOldest
   };

   var mqttServer = MqttServerFactory.CreateBuilder(serverOptions)
      .WithDefaultClientIdGenerator()
      .UseTcp(Port)
      .Build();

   // Hook: Load Retained Messages from JSON at startup
   using var loadRetainedToken = mqttServer.Events.OnLoadingRetainedMessages.Add((context, ct) =>
   {
      var loaded = ServerPersistenceHelper.LoadRetainedMessages(RetainedMessagesFilePath);
      context.LoadedRetainedMessages = loaded;
      return ValueTask.CompletedTask;
   });

   // Hook: Save Retained Messages to JSON whenever they change
   using var saveRetainedToken = mqttServer.Events.OnRetainedMessageChanged.Add((context, ct) =>
   {
      ServerPersistenceHelper.SaveRetainedMessages(RetainedMessagesFilePath, context.StoredRetainedMessages);
      return ValueTask.CompletedTask;
   });

   var startResult = await mqttServer.StartAsync();
   if (startResult.Failed)
   {
      throw new InvalidOperationException($"Server failed to start: {startResult.Error.Detail}");
   }
   TraceLogger.LogServerInfo("Server: Listening on port {0} with persistent sessions enabled.", Port);

   // =====================================================================
   // SECTION 2: Persistent Session and Server-Side Offline Queueing
   // =====================================================================
   TraceLogger.LogInfo("\n--- [Step 2] Setup Persistent Dashboard Client (Subscriber) ---");

   await using var dashboardClient = MqttClientFactory.CreateTcp();

   // Event handler for Dashboard Client receiving messages
   using var receiveToken = dashboardClient.AddMessageReceiveHandler((context, ct) =>
   {
      var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
      TraceLogger.LogClientInfo("[Dashboard] Received message on topic '{0}': {1}", context.Message.Topic, payload);
      return ValueTask.CompletedTask;
   });

   var dashboardConnectOptions = new ConnectOptions
   {
      EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
      ProtocolVersion = MqttProtocolVersion.V50,
      ClientIdUtf8Bytes = Encoding.UTF8.GetBytes("Dashboard-Client"),
      CleanSession = false, // Request persistent session
      SessionExpiryInterval = 300 // Keep session for 300 seconds after disconnect
   };

   TraceLogger.LogClientInfo("Dashboard Client: Connecting with a persistent session (CleanSession = false)...");
   var dashboardConnectResult = await dashboardClient.ConnectAsync(dashboardConnectOptions);
   if (dashboardConnectResult.Failed)
   {
      throw new InvalidOperationException($"Dashboard Client failed to connect: {dashboardConnectResult.Error.Detail}");
   }

   // Subscribe to topics
   var subOptions = SubscribeOptions.Create()
      .WithTopicFilter("device/events", QualityOfServiceType.AtLeastOnce)
      .WithTopicFilter("device/status", QualityOfServiceType.AtLeastOnce)
      .Build();

   var subResult = await dashboardClient.SubscribeAsync(subOptions);
   if (subResult.Failed)
   {
      throw new InvalidOperationException($"Dashboard Client failed to subscribe: {subResult.Error.Detail}");
   }
   TraceLogger.LogClientInfo("Dashboard Client: Subscribed to 'device/events' (QoS 1) and 'device/status' (QoS 1).");

   // =====================================================================
   // SECTION 3: Retained Message Persistence Demonstration
   // =====================================================================
   TraceLogger.LogInfo("\n--- [Step 3] Sensor Publishes a Retained Message ---");

   await using var sensorClient = MqttClientFactory.CreateTcp();
   var sensorConnectOptions = new ConnectOptions
   {
      EndPoint = new IPEndPoint(IPAddress.Loopback, Port),
      ProtocolVersion = MqttProtocolVersion.V50
   };

   await sensorClient.ConnectAsync(sensorConnectOptions);

   var initialStatusPub = PublishOptions.Create()
      .WithTopic("device/status")
      .WithPayload("{ \"status\": \"online\", \"battery\": 100 }")
      .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
      .WithRetain()
      .Build();

   TraceLogger.LogClientInfo("Sensor Client: Publishing initial status with Retain = true...");
   await sensorClient.PublishAsync(initialStatusPub);
   await Task.Delay(200); // Give server a moment to fire Event and write file

   // Confirm JSON file was created
   if (File.Exists(RetainedMessagesFilePath))
   {
      TraceLogger.LogInfo("File Verification: 'retained_messages.json' successfully persisted on disk.");
   }

   // =====================================================================
   // SECTION 4: Server-Side Offline Queueing (Persistent Session)
   // =====================================================================
   TraceLogger.LogInfo("\n--- [Step 4] Simulating Dashboard Client going offline ---");

   // Intentionally disconnect client while keeping the session on the server
   var disconnectOpts = new DisconnectOptions { SessionExpiryInterval = 300 };
   await dashboardClient.DisconnectAsync(disconnectOpts);
   TraceLogger.LogClientInfo("Dashboard Client: Disconnected. Persistent session is now kept offline by server.");
   await Task.Delay(200);

   // Sensor publishes QoS 1 message to 'device/events' while Dashboard is offline
   var offlineEventPub = PublishOptions.Create()
      .WithTopic("device/events")
      .WithPayload("{ \"event\": \"motion_detected\", \"seq\": 1 }")
      .WithQualityOfService(QualityOfServiceType.AtLeastOnce)
      .Build();

   TraceLogger.LogClientInfo("Sensor Client: Publishing event '{{ \"event\": \"motion_detected\" }}' (QoS 1) while Dashboard is offline...");
   await sensorClient.PublishAsync(offlineEventPub);
   await Task.Delay(200);

   // =====================================================================
   // SECTION 5: Client-Side Offline Buffering (FIFO vs Last Message Buffer)
   // =====================================================================
   TraceLogger.LogInfo("\n--- [Step 5] Simulating Client-Side Offline Queueing & Last Message Buffer ---");

   // We wrap the Sensor Client in a helper that implements client-side buffering
   var bufferedSensorClient = new BufferedMqttClient(sensorClient, ClientOfflineQueueFilePath);

   // Simulate connection loss by stopping the Sensor client
   await sensorClient.DisconnectAsync(new DisconnectOptions());
   TraceLogger.LogClientInfo("Sensor Client: Disconnected (simulating local connection loss).");
   await Task.Delay(200);

   // Queue events while offline (FIFO behavior)
   await bufferedSensorClient.PublishBufferedAsync("device/events", "{ \"event\": \"temp_read\", \"seq\": 2 }", QualityOfServiceType.AtLeastOnce, isStateMessage: false);
   await bufferedSensorClient.PublishBufferedAsync("device/events", "{ \"event\": \"temp_read\", \"seq\": 3 }", QualityOfServiceType.AtLeastOnce, isStateMessage: false);

   // Queue status updates while offline (Last Message Buffer state cache behavior)
   await bufferedSensorClient.PublishBufferedAsync("device/status", "{ \"status\": \"busy\" }", QualityOfServiceType.AtLeastOnce, isStateMessage: true);
   await bufferedSensorClient.PublishBufferedAsync("device/status", "{ \"status\": \"idle\" }", QualityOfServiceType.AtLeastOnce, isStateMessage: true);

   // Save offline queue to disk to demonstrate crash safety
   bufferedSensorClient.SaveToFile();
   
   // Clear buffers in memory and restore from file to simulate crash & restart recovery
   bufferedSensorClient.LoadFromFile();

   // =====================================================================
   // SECTION 6: Reconnection and Delivery Verification
   // =====================================================================
   TraceLogger.LogInfo("\n--- [Step 6] Reconnecting Dashboard & Sensor Clients ---");

   TraceLogger.LogClientInfo("Dashboard Client: Reconnecting to restore persistent session...");
   await dashboardClient.ConnectAsync(dashboardConnectOptions);

   // Sensor client reconnects, triggering auto-flush of buffered messages
   TraceLogger.LogClientInfo("Sensor Client: Reconnecting to server...");
   await sensorClient.ConnectAsync(sensorConnectOptions);

   // Wait for delivery & flushing to complete
   await Task.Delay(1500);

   // =====================================================================
   // SECTION 7: Server Restart & Retained Messages JSON Restore Verification
   // =====================================================================
   TraceLogger.LogInfo("\n--- [Step 7] Testing Server Restart & Retained Message Restore ---");

   // Clean disconnect of remaining clients
   await dashboardClient.DisconnectAsync(new DisconnectOptions());
   await sensorClient.DisconnectAsync(new DisconnectOptions());

   TraceLogger.LogServerInfo("Server: Stopping server...");
   await mqttServer.StopAsync();
   await Task.Delay(500);

   TraceLogger.LogServerInfo("Server: Starting new server instance...");
   var newMqttServer = MqttServerFactory.CreateBuilder(serverOptions)
      .WithDefaultClientIdGenerator()
      .UseTcp(Port)
      .Build();

   // Hook: Load Retained Messages from JSON
   using var loadRetainedTokenNew = newMqttServer.Events.OnLoadingRetainedMessages.Add((context, ct) =>
   {
      var loaded = ServerPersistenceHelper.LoadRetainedMessages(RetainedMessagesFilePath);
      context.LoadedRetainedMessages = loaded;
      return ValueTask.CompletedTask;
   });

   await newMqttServer.StartAsync();
   TraceLogger.LogServerInfo("Server: Started.");

   // Connect a fresh client, subscribe to device/status and check if it receives the restored retained message
   await using var verifierClient = MqttClientFactory.CreateTcp();
   using var verifierReceiveToken = verifierClient.AddMessageReceiveHandler((context, ct) =>
   {
      var payload = Encoding.UTF8.GetString(context.Message.Payload.Span);
      TraceLogger.LogClientInfo("[Verifier] Received restored retained message on topic '{0}': {1}", context.Message.Topic, payload);
      return ValueTask.CompletedTask;
   });

   await verifierClient.ConnectAsync(sensorConnectOptions);
   var verifierSub = SubscribeOptions.Create()
      .WithTopicFilter("device/status", QualityOfServiceType.AtLeastOnce)
      .Build();

   TraceLogger.LogClientInfo("Verifier Client: Subscribing to topic 'device/status' to fetch retained message...");
   await verifierClient.SubscribeAsync(verifierSub);

   await Task.Delay(1000);

   // Cleanup
   await verifierClient.DisconnectAsync(new DisconnectOptions());
   await newMqttServer.StopAsync();
}
finally
{
   // Cleanup persistent JSON files
   if (File.Exists(RetainedMessagesFilePath)) File.Delete(RetainedMessagesFilePath);
   if (File.Exists(ClientOfflineQueueFilePath)) File.Delete(ClientOfflineQueueFilePath);
}

Console.WriteLine();
Console.WriteLine("==========================================================");
Console.WriteLine(" Disconnection Safety Example Finished Successfully       ");
Console.WriteLine("==========================================================");


// =====================================================================
// LOCAL CONSOLE ONLY LOGGER WRAPPER
// =====================================================================

public static class TraceLogger
{
   public static void LogInfo(string format, params object?[] arg) => Console.WriteLine(format, arg);
   public static void LogServerInfo(string format, params object?[] arg) => Console.WriteLine("[Server] " + format, arg);
   public static void LogServerError(string format, params object?[] arg) => Console.WriteLine("[Server Error] " + format, arg);
   public static void LogClientInfo(string format, params object?[] arg) => Console.WriteLine("[Client] " + format, arg);
   public static void LogClientWarning(string format, params object?[] arg) => Console.WriteLine("[Client Warning] " + format, arg);
   public static void LogClientError(string format, params object?[] arg) => Console.WriteLine("[Client Error] " + format, arg);
}


// =====================================================================
// PERSISTENCE AND BUFFERING HELPERS
// =====================================================================

public class RetainedMessageDto
{
   public string Topic { get; set; } = string.Empty;
   public string PayloadBase64 { get; set; } = string.Empty;
   public int QualityOfService { get; set; }
   public bool Retain { get; set; }
   public uint MessageExpiryInterval { get; set; }
   public int PayloadFormat { get; set; }
   public string? ResponseTopic { get; set; }
   public string? ContentType { get; set; }
}

public class SavedPublishDto
{
   public string Topic { get; set; } = string.Empty;
   public string PayloadBase64 { get; set; } = string.Empty;
   public int QualityOfService { get; set; }
   public bool Retain { get; set; }
   public bool IsStateMessage { get; set; } // true for Last Message Buffer, false for FIFO Queue
}

public static class ServerPersistenceHelper
{
   private static readonly Lock _lock = new();

   public static void SaveRetainedMessages(string path, IEnumerable<MqttPublishMessage> messages)
   {
      lock (_lock)
      {
         var dtos = messages.Select(m => new RetainedMessageDto
         {
            Topic = m.Topic,
            PayloadBase64 = Convert.ToBase64String(m.Payload.ToArray()),
            QualityOfService = (int)m.QualityOfService,
            Retain = m.Retain,
            MessageExpiryInterval = m.MessageExpiryInterval,
            PayloadFormat = (int)m.PayloadFormat,
            ResponseTopic = m.ResponseTopic,
            ContentType = m.ContentType
         }).ToList();

         var json = JsonSerializer.Serialize(dtos, new JsonSerializerOptions { WriteIndented = true });
         File.WriteAllText(path, json);
         TraceLogger.LogServerInfo("ServerPersistence: Saved {0} retained messages to '{1}'", dtos.Count, path);
      }
   }

   public static List<MqttPublishMessage> LoadRetainedMessages(string path)
   {
      lock (_lock)
      {
         if (!File.Exists(path)) return [];

         try
         {
            var json = File.ReadAllText(path);
            var dtos = JsonSerializer.Deserialize<List<RetainedMessageDto>>(json);
            if (dtos == null) return [];

            var messages = new List<MqttPublishMessage>();
            foreach (var dto in dtos)
            {
               var packet = new PublishPacket
               {
                  Dup = false,
                  QualityOfService = (QualityOfServiceType)dto.QualityOfService,
                  Retain = dto.Retain,
                  TopicUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(dto.Topic)),
                  Payload = new ReadOnlySequence<byte>(Convert.FromBase64String(dto.PayloadBase64)),
                  MessageExpiryInterval = dto.MessageExpiryInterval,
                  PayloadFormat = (PayloadFormat)dto.PayloadFormat,
                  ResponseTopicUtf8Bytes = string.IsNullOrEmpty(dto.ResponseTopic)
                     ? ReadOnlySequence<byte>.Empty
                     : new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(dto.ResponseTopic)),
                  ContentTypeUtf8Bytes = string.IsNullOrEmpty(dto.ContentType)
                     ? ReadOnlySequence<byte>.Empty
                     : new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(dto.ContentType)),
               };
               messages.Add(new MqttPublishMessage(packet));
            }

            TraceLogger.LogServerInfo("ServerPersistence: Successfully restored {0} retained messages from '{1}'", messages.Count, path);
            return messages;
         }
         catch (Exception ex)
         {
            TraceLogger.LogServerError("ServerPersistence: Failed to load retained messages: {0}", ex.Message);
            return [];
         }
      }
   }
}

public class BufferedMqttClient
{
   private readonly IMqttClient _client;
   private readonly string _persistencePath;
   private readonly ConcurrentQueue<SavedPublishDto> _fifoQueue = new();
   private readonly ConcurrentDictionary<string, SavedPublishDto> _lastMessageBuffer = new();
   private readonly Lock _lock = new();

   public BufferedMqttClient(IMqttClient client, string persistencePath)
   {
      _client = client;
      _persistencePath = persistencePath;

      // Automatically flush on reconnection
      _client.AddConnectedHandler(async (context, ct) =>
      {
         TraceLogger.LogClientInfo("BufferedClient: Connection detected. Flushing offline buffers...");
         _ = Task.Run(async () => await FlushAsync(), ct);
      });
   }

   public async Task PublishBufferedAsync(string topic, string payload, QualityOfServiceType qos, bool retain = false, bool isStateMessage = false)
   {
      var dto = new SavedPublishDto
      {
         Topic = topic,
         PayloadBase64 = Convert.ToBase64String(Encoding.UTF8.GetBytes(payload)),
         QualityOfService = (int)qos,
         Retain = retain,
         IsStateMessage = isStateMessage
      };

      if (_client.IsConnected)
      {
         await SendDtoAsync(dto);
      }
      else
      {
         if (isStateMessage)
         {
            _lastMessageBuffer[topic] = dto;
            TraceLogger.LogClientWarning("BufferedClient: [State Cache] Buffered latest state message for topic '{0}': {1}", topic, payload);
         }
         else
         {
            _fifoQueue.Enqueue(dto);
            TraceLogger.LogClientWarning("BufferedClient: [FIFO Queue] Enqueued event message for topic '{0}': {1}", topic, payload);
         }
      }
   }

   public void SaveToFile()
   {
      lock (_lock)
      {
         var list = new List<SavedPublishDto>();
         list.AddRange(_fifoQueue);
         list.AddRange(_lastMessageBuffer.Values);

         var json = JsonSerializer.Serialize(list, new JsonSerializerOptions { WriteIndented = true });
         File.WriteAllText(_persistencePath, json);
         TraceLogger.LogClientInfo("BufferedClient: Saved {0} offline messages to '{1}'.", list.Count, _persistencePath);
      }
   }

   public void LoadFromFile()
   {
      lock (_lock)
      {
         if (!File.Exists(_persistencePath)) return;

         try
         {
            var json = File.ReadAllText(_persistencePath);
            var list = JsonSerializer.Deserialize<List<SavedPublishDto>>(json);
            if (list != null)
            {
               _fifoQueue.Clear();
               _lastMessageBuffer.Clear();

               foreach (var dto in list)
               {
                  if (dto.IsStateMessage)
                  {
                     _lastMessageBuffer[dto.Topic] = dto;
                  }
                  else
                  {
                     _fifoQueue.Enqueue(dto);
                  }
               }
               TraceLogger.LogClientInfo("BufferedClient: Restored {0} offline messages from '{1}'.", list.Count, _persistencePath);
            }
         }
         catch (Exception ex)
         {
            TraceLogger.LogClientError("BufferedClient: Failed to load offline messages: {0}", ex.Message);
         }
      }
   }

   private async Task FlushAsync()
   {
      // 1. Flush FIFO Queue (Events)
      while (_fifoQueue.TryDequeue(out var dto))
      {
         try
         {
            await SendDtoAsync(dto);
         }
         catch (Exception ex)
         {
            TraceLogger.LogClientError("BufferedClient: Failed to flush message to '{0}': {1}. Re-enqueueing.", dto.Topic, ex.Message);
            _fifoQueue.Enqueue(dto);
            return; // stop flushing if network goes down again
         }
      }

      // 2. Flush Last Message Buffer (State Cache)
      var topics = _lastMessageBuffer.Keys.ToArray();
      foreach (var topic in topics)
      {
         if (_lastMessageBuffer.TryRemove(topic, out var dto))
         {
            try
            {
               await SendDtoAsync(dto);
            }
            catch (Exception ex)
            {
               TraceLogger.LogClientError("BufferedClient: Failed to flush state message to '{0}': {1}. Re-buffering.", topic, ex.Message);
               _lastMessageBuffer[topic] = dto;
               return;
            }
         }
      }

      // Delete persistence file once successfully flushed
      if (File.Exists(_persistencePath))
      {
         try
         {
            File.Delete(_persistencePath);
            TraceLogger.LogClientInfo("BufferedClient: Offline persistence file '{0}' cleared.", _persistencePath);
         }
         catch { /* ignored */ }
      }
   }

   private async Task SendDtoAsync(SavedPublishDto dto)
   {
      var payloadBytes = Convert.FromBase64String(dto.PayloadBase64);
      var options = PublishOptions.Create()
         .WithTopic(dto.Topic)
         .WithPayload(payloadBytes)
         .WithQualityOfService((QualityOfServiceType)dto.QualityOfService)
         .WithRetain(dto.Retain)
         .Build();

      TraceLogger.LogClientInfo("BufferedClient: Publishing flushed message to topic '{0}'...", dto.Topic);
      var result = await _client.PublishAsync(options);
      if (result.Failed)
      {
         throw new Exception(result.Error.Detail);
      }
   }
}
