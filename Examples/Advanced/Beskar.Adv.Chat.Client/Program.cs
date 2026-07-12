using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Text;
using Beskar.Adv.Chat.Common;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;

Console.WriteLine("=================================================");
Console.WriteLine("Welcome to Beskar Chat Client!");
Console.WriteLine("=================================================");
Console.WriteLine("Select transport protocol to connect:");
Console.WriteLine("1. TCP (Port 9000)");
Console.WriteLine("2. WebSocket (Port 11000)");
Console.WriteLine("3. QUIC (Port 12000)");
Console.Write("Enter choice (1-3): ");

var choice = Console.ReadLine();
INetworkClient client;
IPEndPoint endPoint;

if (choice == "1")
{
   client = new TcpNetworkClient(new TcpTransportOptions { NoDelay = true });
   endPoint = new IPEndPoint(IPAddress.Loopback, 9000);
}
else if (choice == "2")
{
   client = new WsNetworkClient(new WsTransportOptions { TcpOptions = new TcpTransportOptions { NoDelay = true } });
   endPoint = new IPEndPoint(IPAddress.Loopback, 11000);
}
else if (choice == "3")
{
   if (!QuicConnection.IsSupported)
   {
      Console.WriteLine("QUIC is not supported on this platform.");
      return;
   }

   var sslOptions = new SslClientAuthenticationOptions
   {
      ApplicationProtocols = [new SslApplicationProtocol("beskar-chat")],
      RemoteCertificateValidationCallback = (sender, cert, chain, errors) => true
   };

   client = new QuicNetworkClient(new QuicTransportOptions
   {
      AlpnProtocol = "beskar-chat",
      SslClientOptions = sslOptions
   });
   endPoint = new IPEndPoint(IPAddress.Loopback, 12000);
}
else
{
   Console.WriteLine("Invalid choice. Exiting.");
   return;
}

Console.WriteLine($"[Client] Connecting to {endPoint}...");
var cts = new CancellationTokenSource();
var ct = cts.Token;

var connectResult = await client.ConnectAsync(endPoint, ct);
if (connectResult.Failed)
{
   Console.WriteLine($"[Client] Connection failed: {connectResult.Error.Message}");
   return;
}

var session = connectResult.Success;
Console.WriteLine("[Client] Session established. Opening stream...");

var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
if (streamResult.Failed)
{
   Console.WriteLine($"[Client] Failed to open stream: {streamResult.Error.Message}");
   await session.DisposeAsync();
   return;
}

var stream = streamResult.Success;
var channel = new MessageChannel(stream.Transport);

// Handshake - Join chat
var username = NameGenerator.Generate();
Console.WriteLine($"[Client] Joining chat as: {username}");

await channel.WritePacketAsync(ChatPacket.CreateJson(PacketType.Join, new JoinPayload { Username = username }), ct);

var welcomePacket = await channel.ReadPacketAsync(ct);
if (welcomePacket is null || welcomePacket.Type != PacketType.Welcome)
{
   Console.WriteLine("[Client] Invalid handshake response from server.");
   await session.DisposeAsync();
   return;
}

var welcome = welcomePacket.AsJson<WelcomePayload>();
if (welcome is null)
{
   Console.WriteLine("[Client] Failed to parse welcome packet.");
   await session.DisposeAsync();
   return;
}

Console.WriteLine("=================================================");
Console.WriteLine($"Joined Chat Room as: {welcome.Username}");
Console.WriteLine("=================================================");

if (welcome.History.Count > 0)
{
   Console.WriteLine("--- Chat History ---");
   foreach (var msg in welcome.History)
   {
      var time = msg.Timestamp.ToString("HH:mm:ss");
      if (msg.Sender == "System")
      {
         Console.WriteLine($"[System] {msg.Text}");
      }
      else
      {
         Console.WriteLine($"[{time}] {msg.Sender}: {msg.Text}");
      }
   }

   Console.WriteLine("--------------------");
}

var consoleLock = new Lock();
var inputBuilder = new StringBuilder();

var receiveTask = Task.Run(async () =>
{
   try
   {
      while (!ct.IsCancellationRequested)
      {
         var packet = await channel.ReadPacketAsync(ct);
         if (packet is null)
         {
            lock (consoleLock)
            {
               Console.WriteLine("\n[System] Disconnected from server.");
            }

            break;
         }

         if (packet.Type is not PacketType.Message) continue;

         var msg = packet.AsJson<ChatMessage>();
         if (msg is null) continue;

         var time = msg.Timestamp.ToString("HH:mm:ss");
         lock (consoleLock)
         {
            var currentInput = inputBuilder.ToString();
            Console.Write("\r" + new string(' ', currentInput.Length + 2) + "\r");

            Console.WriteLine(msg.Sender == "System"
               ? $"[System] {msg.Text}"
               : $"[{time}] {msg.Sender}: {msg.Text}");

            Console.Write("> " + currentInput);
         }
      }
   }
   catch (Exception ex)
   {
      lock (consoleLock)
      {
         Console.WriteLine($"\n[System] Error in receive: {ex.Message}");
      }
   }
});

Console.WriteLine("Type your message and press Enter. Type '/exit' to quit.");
Console.Write("> ");

while (!ct.IsCancellationRequested)
{
   if (Console.KeyAvailable)
   {
      var keyInfo = Console.ReadKey(intercept: true);
      if (keyInfo.Key == ConsoleKey.Enter)
      {
         var input = string.Empty;
         lock (consoleLock)
         {
            input = inputBuilder.ToString();
            inputBuilder.Clear();

            Console.WriteLine();
         }

         if (input == "/exit")
         {
            break;
         }

         if (!string.IsNullOrWhiteSpace(input))
         {
            var chatMsg = new ChatMessage { Text = input };
            try
            {
               await channel.WritePacketAsync(ChatPacket.CreateJson(PacketType.Message, chatMsg), ct);
            }
            catch (Exception ex)
            {
               Console.WriteLine($"[Client] Error sending message: {ex.Message}");
               break;
            }
         }

         lock (consoleLock)
         {
            Console.Write("> ");
         }
      }
      else if (keyInfo.Key == ConsoleKey.Backspace)
      {
         lock (consoleLock)
         {
            if (inputBuilder.Length > 0)
            {
               inputBuilder.Length--;
               Console.Write("\b \b");
            }
         }
      }
      else
      {
         lock (consoleLock)
         {
            inputBuilder.Append(keyInfo.KeyChar);
            Console.Write(keyInfo.KeyChar);
         }
      }
   }
   else
   {
      await Task.Delay(50, ct);
   }
}

Console.WriteLine("[Client] Disconnecting...");
await cts.CancelAsync();
try
{
   await receiveTask;
}
catch
{
   // Ignored
}

await client.DisconnectAsync();
await session.DisposeAsync();
Console.WriteLine("[Client] Disconnected.");
