using System.Collections.Concurrent;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Resilient.Server;
using Beskar.Networking.Resilient.Server.Models;
using Beskar.Resilient.Chat.Common;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;

Console.WriteLine("=================================================");
Console.WriteLine("Starting Resilient Chat Server...");
Console.WriteLine("=================================================");

var serverOptions = new ResilientServerOptions
{
   Serializer = new ChatSerializer()
};

var server = ResilientServerFactory.CreateBuilder(serverOptions)
   .UseTcp(9000)
   .Build();

// Client connection store: Map Client ID to Username
var clients = new ConcurrentDictionary<Guid, (string Username, ResilientServerClient<BeskarPacket> Client)>();
var history = new List<ChatMessage>();
var historyLock = new object();

// Handle client messages and actions
server.Events.FrameReceived.Add(async (ctx, ct) =>
{
   var client = ctx.Client;
   var envelope = client.DeserializePayload<ChatPacketEnvelope>(ctx.Frame);
   if (envelope is null) return;

   if (envelope.Type == ChatPacketType.Join)
   {
      var joinPayload = envelope.GetPayload<JoinPayload>();
      if (joinPayload is null || string.IsNullOrWhiteSpace(joinPayload.Username)) return;

      var username = joinPayload.Username;
      Console.WriteLine($"[Server] Client joined: {username} ({client.Session.RemoteAddress})");

      // Retrieve history and send welcome packet
      List<ChatMessage> historySnapshot;
      lock (historyLock)
      {
         historySnapshot = [.. history];
      }

      var welcomePayload = new WelcomePayload
      {
         Username = username,
         History = historySnapshot
      };

      await client.SendPayloadAsync(ChatPacketEnvelope.Create(ChatPacketType.Welcome, welcomePayload),
         cancellationToken: ct);

      // Register the client
      clients[client.Id] = (username, client);

      // Broadcast join message
      var joinMsg = new ChatMessage
      {
         Sender = "System",
         Text = $"{username} joined the chat.",
         Timestamp = DateTime.Now
      };
      await BroadcastMessageAsync(joinMsg, ct);
   }
   else if (envelope.Type == ChatPacketType.Message)
   {
      var chatMessage = envelope.GetPayload<ChatMessage>();
      if (chatMessage is null) return;

      if (clients.TryGetValue(client.Id, out var clientInfo))
      {
         chatMessage.Sender = clientInfo.Username;
         chatMessage.Timestamp = DateTime.Now;

         // Add to history
         lock (historyLock)
         {
            history.Add(chatMessage);
            if (history.Count > 50) history.RemoveAt(0);
         }

         // Broadcast message
         await BroadcastMessageAsync(chatMessage, ct);
      }
   }
});

// Handle client disconnections
server.Events.ClientDisconnected.Add(async (ctx, ct) =>
{
   if (clients.TryRemove(ctx.Client.Id, out var clientInfo))
   {
      Console.WriteLine($"[Server] Client left: {clientInfo.Username}");

      var leaveMsg = new ChatMessage
      {
         Sender = "System",
         Text = $"{clientInfo.Username} left the chat.",
         Timestamp = DateTime.Now
      };
      await BroadcastMessageAsync(leaveMsg, ct);
   }
});

var serverTask = Task.Run(async () =>
{
   try
   {
      var result = await server.StartAsync();
      if (result.Failed) Console.WriteLine($"[System] Failed to start server: {result.Error.Detail}");
   }
   catch (Exception ex)
   {
      Console.WriteLine($"[System] Server error: {ex.Message}");
   }
});

Console.WriteLine("[System] Resilient Chat Server is running. Press Enter to stop.");
Console.ReadLine();

Console.WriteLine("[System] Shutting down Server...");
await server.DisposeAsync();

try
{
   await serverTask;
}
catch (OperationCanceledException)
{
   // Expected
}

Console.WriteLine("[System] Resilient Chat Server stopped.");

async Task BroadcastMessageAsync(ChatMessage message, CancellationToken ct)
{
   var envelope = ChatPacketEnvelope.Create(ChatPacketType.Message, message);
   var tasks = clients.Values.Select(async clientInfo =>
   {
      try
      {
         await clientInfo.Client.SendPayloadAsync(envelope, cancellationToken: ct);
      }
      catch
      {
         // Ignored
      }
   });

   await Task.WhenAll(tasks);
}
