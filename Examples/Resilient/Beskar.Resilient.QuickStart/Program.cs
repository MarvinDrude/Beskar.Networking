using System.Buffers;
using System.Net;
using System.Text;
using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Frames;
using Beskar.Networking.Resilient.Client;
using Beskar.Networking.Resilient.Server;

var endPoint = new IPEndPoint(IPAddress.Loopback, 9005);

// 1. Resilient Server
var server = ResilientServerFactory.CreateBuilder().UseTcp(endPoint).Build();
server.Events.FrameReceived.Add((ctx, ct) =>
{
   var text = Encoding.UTF8.GetString(ctx.Frame.GetPayloadSequence().ToArray());
   Console.WriteLine($"Server received: {text}");

   // Echo response frame back
   return ctx.Client.SendAsync(BeskarPacket.CreateMessage("Pong!"u8.ToArray()), ct);
});
await server.StartAsync();

// 2. Resilient Client (with Auto-Reconnect enabled)
var client = ResilientClientFactory.CreateTcp<BeskarPacket>(clientOptions: new ResilientClientOptions
{
   Reconnecting = new ResilientClientReconnectionOptions { AutoReconnect = true }
});

client.Events.FrameReceived.Add((ctx, ct) =>
{
   Console.WriteLine($"Client received: {Encoding.UTF8.GetString(ctx.Frame.GetPayloadSequence().ToArray())}");
   return ValueTask.CompletedTask;
});

await client.ConnectAsync(endPoint);
await client.SendAsync(BeskarPacket.CreateMessage("Ping!"u8.ToArray()));

await Task.Delay(500);

await client.DisposeAsync();
await server.DisposeAsync();
