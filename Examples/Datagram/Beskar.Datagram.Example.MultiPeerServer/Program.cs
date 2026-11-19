using System.Net;
using System.Text;
using Beskar.Networking.Datagram;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;

static DatagramOptions CreateOptions(bool manualTick)
   => new DatagramOptions
   {
      ManualTick = manualTick,
      DispatchMode = manualTick ? DatagramEventDispatchMode.Poll : DatagramEventDispatchMode.Async,
      Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Aead }
   }
   .AddChannel(0, DatagramDeliveryMode.ReliableOrdered, priority: 255)   // Lifecycle / turns / RPCs
   .AddChannel(1, DatagramDeliveryMode.ReliableUnordered, priority: 200) // Independent events
   .AddChannel(2, DatagramDeliveryMode.UnreliableSequenced, priority: 150) // High-frequency state snapshots
   .AddChannel(3, DatagramDeliveryMode.UnreliableUnordered, priority: 100) // Ephemeral FX cues
   .AddChannel(4, DatagramDeliveryMode.ReliableSequenced, priority: 180); // Latest-only authoritative state

await using var server = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), CreateOptions(manualTick: false));
server.Events.ValidateConnection = ctx =>
{
   var name = Encoding.UTF8.GetString(ctx.ConnectMetadata.Span);
   ctx.PeerTag = string.IsNullOrWhiteSpace(name) ? "anonymous" : name;
   return ValueTask.CompletedTask;
};
server.Events.PeerConnected = (peer, _) =>
{
   Console.WriteLine($"[Server] Peer #{peer.PeerId} connected ({peer.Tag}) from {peer.RemoteEndPoint}");
   return ValueTask.CompletedTask;
};
server.Events.MessageReceived = (peer, ch, mode, payload, _) =>
{
   Console.WriteLine($"[Server] From {peer.Tag} (#{peer.PeerId}) on Ch {ch} ({mode}): {Encoding.UTF8.GetString(payload.Span)}");
   if (ch == 0)
   {
      server.Broadcast(0, Encoding.UTF8.GetBytes($"Broadcast from {peer.Tag}"), exclude: peer);
   }
   return ValueTask.CompletedTask;
};
server.Start();

await using var client1 = new DatagramHost(CreateOptions(manualTick: true));
await using var client2 = new DatagramHost(CreateOptions(manualTick: true));

var c1Peer = await client1.ConnectAsync(server.LocalEndPoint!, "Alice"u8.ToArray());
var c2Peer = await client2.ConnectAsync(server.LocalEndPoint!, "Bob"u8.ToArray());

c1Peer.TrySend(0, "Hello everyone!"u8);
c2Peer.TrySend(2, "pos:(12.4, 3.1)"u8);

client1.Tick();
client2.Tick();

await Task.Delay(100);
client1.Tick();
client2.Tick();

client2.PollEvents(evt =>
{
   if (evt.Type == DatagramEventType.MessageReceived)
   {
      Console.WriteLine($"[Bob] Received on Ch {evt.ChannelId}: {Encoding.UTF8.GetString(evt.Payload.Span)}");
   }
});
