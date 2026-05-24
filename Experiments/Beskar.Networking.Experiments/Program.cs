using System.Buffers;
using System.Net;
using System.Text;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Common.Hosting;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Tcp.Extensions;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Quic.Extensions;
using Beskar.Networking.Transports.Ws;
using Beskar.Networking.Transports.Ws.Extensions;
using Beskar.Utilities.Console.Rendering;

Console.Clear();
ConsoleRender.DrawHeader(
    "BESKAR NETWORKING EXPERIMENTS",
    "Sleek TCP/QUIC Client/Server Connection Playground",
    BoxStyle.Rounded,
    ConsoleColor.Yellow
);

var protocolKey = ConsoleRender.AskChoice("Select Transport Protocol", new[] { "TCP", "QUIC", "WebSocket" }, defaultChoice: "t");
var isQuic = protocolKey == "q";
var isWebSocket = protocolKey == "w";
var protocolName = isWebSocket ? "WebSocket" : (isQuic ? "QUIC" : "TCP");

var portStr = ConsoleRender.AskString("Enter server port number", defaultValue: "1337");
if (!int.TryParse(portStr, out var port))
{
    port = 1337;
}

var server = NetworkServerBuilder.Create()
   .ConfigureServers(register =>
   {
      register.ListenLocalhost(port, options =>
      {
         if (isWebSocket)
         {
            options.UseWebSocket();
         }
         else if (isQuic)
         {
            options.UseQuic();
         }
         else
         {
            options.UseTcp();
         }

         options.OnSession(async session =>
         {
            ConsoleRender.Server($"Session accepted: {session.Id}");
            var streamResult = await session.AcceptStreamAsync();

            if (streamResult.IsSuccess)
            {
               var stream = streamResult.Success!;
               var reader = stream.Transport.Input;
               try
               {
                  while (true)
                  {
                     var result = await reader.ReadAsync();
                     var buffer = result.Buffer;

                     if (buffer.IsEmpty && result.IsCompleted)
                     {
                        break;
                     }

                     if (!buffer.IsEmpty)
                     {
                        var message = Encoding.UTF8.GetString(buffer.ToArray());
                        ConsoleRender.Server($"Received message: [yellow]\"{message}\"[/yellow]");
                     }

                     reader.AdvanceTo(buffer.End);
                  }
               }
               catch (Exception ex)
               {
                  ConsoleRender.Server($"Stream read error: {ex.Message}");
               }
            }
            ConsoleRender.Server($"Session closed: {session.Id}");
         });
      });
   })
   .Build();

var clientBuilder = NetworkClientBuilder.Create();
if (isWebSocket)
{
   clientBuilder.UseWebSocket();
}
else if (isQuic)
{
   clientBuilder.UseQuic();
}
else
{
   clientBuilder.UseTcp();
}

await ConsoleRender.RunSpinnerAsync(
    $"Configuring playground using {protocolName} on port {port}...",
    async () =>
    {
        await server.StartAsync();
        await Task.Delay(200);
    },
    successMessage: $"{protocolName} Server successfully started and listening."
);

INetworkSession? activeSession = null;
INetworkStream? activeStream = null;

var messagesSent = 0;
var bytesSent = 0;

try
{
   while (true)
   {
      var choices = new PromptChoice[]
      {
         new("c", "Connect client to server"),
         new("d", "Disconnect client from server"),
         new("s", "Send a message from client to server"),
         new("t", "Show playground statistics table"),
         new("q", "Quit playground application")
      };

      var cmd = ConsoleRender.AskChoice("Commands available", choices, defaultChoice: "t", vertical: true);

      if (cmd == "q")
      {
         ConsoleRender.Info("Shutting down connection playground...");
         break;
      }

      switch (cmd)
      {
         case "c":
            if (activeSession is not null)
            {
               ConsoleRender.Warning("Client is already connected!");
               break;
            }

            try
            {
               await ConsoleRender.RunSpinnerAsync("Establishing connection to server...", async () =>
               {
                  var connectResult = await clientBuilder.ConnectAsync(new IPEndPoint(IPAddress.Loopback, port));
                  if (connectResult.IsSuccess)
                  {
                     activeSession = connectResult.Success!;
                     var streamResult = await activeSession.OpenStreamAsync();
                     if (streamResult.IsSuccess)
                     {
                        activeStream = streamResult.Success!;
                        // Acknowledge connection and wait a brief delay to let Server's OnSession log print during the active spinner
                        await Task.Delay(200);
                     }
                     else
                     {
                        throw new Exception($"Failed to open client stream: {streamResult.Error.Message}");
                     }
                  }
                  else
                  {
                     throw new Exception($"Connection handshake failed: {connectResult.Error.Message}");
                  }
               }, successMessage: "Client successfully connected and handshake established.");
            }
            catch (Exception ex)
            {
               ConsoleRender.Error($"Handshake error: {ex.Message}");
               await CloseClientAsync();
            }
            break;

         case "d":
            if (activeSession is null)
            {
               ConsoleRender.Error("Client is not currently connected.");
               break;
            }

            await ConsoleRender.RunSpinnerAsync("Disconnecting and closing sessions...", async () =>
            {
               await CloseClientAsync();
               // Wait a brief delay to let Server's OnSession close log print during the active spinner
               await Task.Delay(200);
            }, successMessage: "Client successfully disconnected.");
            break;

         case "s":
            if (activeSession is null || activeStream is null)
            {
               ConsoleRender.Error("Client is not connected or stream is closed.");
               break;
            }

            var msg = ConsoleRender.AskString("Enter message to send", defaultValue: "Hello from active Beskar client!");

            try
            {
               await ConsoleRender.RunSpinnerAsync($"Sending message: \"{msg}\"...", async () =>
               {
                  var writer = activeStream.Transport.Output;
                  var payload = Encoding.UTF8.GetBytes(msg);
                  await writer.WriteAsync(payload);
                  await writer.FlushAsync();

                  messagesSent++;
                  bytesSent += payload.Length;

                  // Wait a brief delay to let Server's OnSession message read log print during the active spinner
                  await Task.Delay(200);
               }, successMessage: "Payload successfully sent and flushed.");
            }
            catch (Exception ex)
            {
               ConsoleRender.Error($"Failed to transmit payload: {ex.Message}");
            }
            break;

         case "t":
            // Render stunning live-statistics table
            var table = ConsoleRender.CreateTable()
               .SetStyle(BoxStyle.Rounded)
               .SetBorderColor(ConsoleColor.DarkCyan)
               .AddColumn("Metric Description", Alignment.Left, ConsoleColor.Yellow)
               .AddColumn("Value", Alignment.Right, ConsoleColor.Cyan);

            table.AddRow("Transport Protocol", protocolName);
            table.AddRow("Port Bound", port.ToString());
            table.AddRow("Connection Status", activeSession is not null ? "[success]ONLINE (Connected)[/]" : "[error]OFFLINE (Disconnected)[/]");
            table.AddRow("Active Session ID", activeSession?.Id.ToString() ?? "None");
            table.AddRow("Messages Sent", messagesSent.ToString());
            table.AddRow("Bytes Exchanged", $"{bytesSent:N0} bytes");

            Console.WriteLine();
            table.Render();
            Console.WriteLine();
            break;
      }
   }
}
finally
{
   await CloseClientAsync();

   await ConsoleRender.RunSpinnerAsync("Stopping and shutting down server listener...", async () =>
   {
      await server.StopAsync();
   }, successMessage: "Server listener successfully stopped.");

   ConsoleRender.Success("Playground session ended. Goodbye!");
}

async Task CloseClientAsync()
{
   if (activeStream is not null)
   {
      try
      {
         await activeStream.DisposeAsync();
      }
      catch { /* Ignored */ }
      activeStream = null;
   }

   if (activeSession is not null)
   {
      if (activeSession is IAsyncDisposable asyncDisposable)
      {
         try
         {
            await asyncDisposable.DisposeAsync();
         }
         catch { /* Ignored */ }
      }
      else if (activeSession is IDisposable disposable)
      {
         disposable.Dispose();
      }
      activeSession = null;
   }
}
