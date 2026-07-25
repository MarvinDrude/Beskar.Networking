using System.Net;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Resilient.Client;
using Beskar.Resilient.Chat.Common;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = false; // Disable trace logging to keep chat UI clean

Console.WriteLine("=================================================");
Console.WriteLine("Welcome to Resilient Chat Client!");
Console.WriteLine("=================================================");

Console.Write("Enter your username: ");
var username = Console.ReadLine();
if (string.IsNullOrWhiteSpace(username)) username = $"User_{Random.Shared.Next(1000, 9999)}";

var clientOptions = new ResilientClientOptions
{
   Serializer = new ChatSerializer(),
   Reconnecting = new ResilientClientReconnectionOptions
   {
      AutoReconnect = true,
      RetryInterval = TimeSpan.FromSeconds(2),
      MaxRetries = 15
   }
};

// Create client using TCP and BeskarPacket framing
var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: clientOptions);

// Setup event handlers
client.Events.OnConnected.Add(async (ctx, ct) =>
{
   Console.WriteLine("\n[System] Connected to chat server. Joining...");
   var joinEnvelope = ChatPacketEnvelope.Create(ChatPacketType.Join, new JoinPayload { Username = username });
   await ctx.Client.SendPayloadAsync(joinEnvelope, cancellationToken: ct);
});

client.Events.OnDisconnected.Add((ctx, ct) =>
{
   Console.WriteLine("\n[System] Disconnected from server.");
   return ValueTask.CompletedTask;
});

client.Events.OnReconnecting.Add((ctx, ct) =>
{
   Console.WriteLine($"\n[System] Connection lost. Reconnecting... (Attempt #{ctx.Attempt})");
   return ValueTask.CompletedTask;
});

client.Events.FrameReceived.Add((ctx, ct) =>
{
   var envelope = ctx.Client.DeserializePayload<ChatPacketEnvelope>(ctx.Frame);
   if (envelope is null) return ValueTask.CompletedTask;

   if (envelope.Type == ChatPacketType.Welcome)
   {
      var welcome = envelope.GetPayload<WelcomePayload>();
      if (welcome != null)
      {
         Console.WriteLine($"[System] Welcome {welcome.Username}! Active history:");
         foreach (var msg in welcome.History) Console.WriteLine($"[{msg.Timestamp:HH:mm:ss}] {msg.Sender}: {msg.Text}");
         Console.WriteLine("---------------------------------------------");
      }
   }
   else if (envelope.Type == ChatPacketType.Message)
   {
      var msg = envelope.GetPayload<ChatMessage>();
      if (msg != null) Console.WriteLine($"[{msg.Timestamp:HH:mm:ss}] {msg.Sender}: {msg.Text}");
   }

   return ValueTask.CompletedTask;
});

var endPoint = new IPEndPoint(IPAddress.Loopback, 9000);
Console.WriteLine($"[System] Connecting to server at {endPoint}...");
var connectResult = await client.ConnectAsync(endPoint);

if (connectResult.Failed) Console.WriteLine($"[System] Initial connection failed: {connectResult.Error.Detail}");

Console.WriteLine("Type messages and press Enter to send (type '/exit' to quit):");
while (true)
{
   var text = Console.ReadLine();
   if (string.IsNullOrWhiteSpace(text)) continue;

   if (text == "/exit") break;

   if (!client.IsConnected)
   {
      Console.WriteLine("[System] Cannot send. Client is currently disconnected.");
      continue;
   }

   try
   {
      var msgEnvelope = ChatPacketEnvelope.Create(ChatPacketType.Message, new ChatMessage { Text = text });
      await client.SendPayloadAsync(msgEnvelope);
   }
   catch (Exception ex)
   {
      Console.WriteLine($"[System] Failed to send message: {ex.Message}");
   }
}

Console.WriteLine("Disconnecting...");
await client.DisposeAsync();
Console.WriteLine("Goodbye!");
