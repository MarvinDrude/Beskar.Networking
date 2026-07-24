using System.Buffers;
using System.Net;
using System.Text;
using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Server;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;

Console.WriteLine("=================================================");
Console.WriteLine("Resilient Ping-Pong Example");
Console.WriteLine("=================================================");

var endPoint = new IPEndPoint(IPAddress.Loopback, 9001);

// 1. Configure and Build the Resilient Server
var server = ResilientServerFactory.CreateBuilder()
   .UseTcp(endPoint)
   .Build();

server.Events.OnStart.Add((ctx, ct) =>
{
   Console.WriteLine("[Server] Resilient Server started and listening.");
   return ValueTask.CompletedTask;
});

server.Events.FrameReceived.Add(async (ctx, ct) =>
{
   var frame = ctx.Frame;
   var text = Encoding.UTF8.GetString(frame.GetPayloadSequence().ToArray());
   Console.WriteLine($"[Server] Received: \"{text}\" from client: {ctx.Client.Id}");

   if (text == "Ping")
   {
      // Respond with Pong
      var pongFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Message,
         new ReadOnlySequence<byte>("Pong"u8.ToArray()));
      await ctx.Client.SendAsync(pongFrame, ct);
      Console.WriteLine("[Server] Sent response: \"Pong\"");
   }
});

await server.StartAsync();

// 2. Configure and Build the Resilient Client
var clientOptions = new ResilientClientOptions
{
   Reconnecting = new ResilientClientReconnectionOptions
   {
      AutoReconnect = true,
      RetryInterval = TimeSpan.FromSeconds(1)
   }
};

var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: clientOptions);

client.Events.OnConnected.Add((ctx, ct) =>
{
   Console.WriteLine("[Client] Connected and handshaked successfully!");
   return ValueTask.CompletedTask;
});

client.Events.FrameReceived.Add((ctx, ct) =>
{
   var frame = ctx.Frame;
   var text = Encoding.UTF8.GetString(frame.GetPayloadSequence().ToArray());
   Console.WriteLine($"[Client] Received: \"{text}\"");
   return ValueTask.CompletedTask;
});

// Connect to the server
var connectResult = await client.ConnectAsync(endPoint);
if (connectResult.Failed)
{
   Console.WriteLine($"[Client] Failed to connect: {connectResult.Error.Detail}");
   return;
}

// 3. Ping-Pong Loop
for (var i = 1; i <= 5; i++)
{
   Console.WriteLine($"\n--- Round {i} ---");
   var pingFrame = BeskarPacket.CreateFrame(ResilientFrameKind.Message,
      new ReadOnlySequence<byte>("Ping"u8.ToArray()));

   Console.WriteLine("[Client] Sending: \"Ping\"");
   await client.SendAsync(pingFrame);

   // Await a short delay before next round
   await Task.Delay(1000);
}

// 4. Graceful Cleanup
Console.WriteLine("\n[System] Cleaning up resources...");
await client.DisposeAsync();
await server.DisposeAsync();
Console.WriteLine("[System] Shutdown complete.");
