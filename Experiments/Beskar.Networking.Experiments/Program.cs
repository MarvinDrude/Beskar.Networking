using System.Buffers;
using System.Net;
using System.Text;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Common.Hosting;
using Beskar.Networking.Transports.Tcp;

Console.Clear();
Console.ForegroundColor = ConsoleColor.DarkYellow;
Console.WriteLine("┌────────────────────────────────────────────────────────┐");
Console.WriteLine("│             BESKAR NETWORKING EXPERIMENTS              │");
Console.WriteLine("│     Sleek TCP Client/Server Connection Playground      │");
Console.WriteLine("└────────────────────────────────────────────────────────┘");
Console.ResetColor();
Console.WriteLine();
Console.WriteLine("Commands available:");
Console.WriteLine("  c - Connect client to server");
Console.WriteLine("  d - Disconnect client from server");
Console.WriteLine("  s - Send a message from client to server");
Console.WriteLine("  q - Quit application");
Console.WriteLine();

var server = NetworkServerBuilder.Create()
   .ConfigureServers(register =>
   {
      register.ListenLocalhost(1337, options =>
      {
         options.UseTcp();
         options.OnSession(async session =>
         {
            LogServer($"Session accepted: {session.Id}");
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
                        LogServer($"Received message: \"{message}\"");
                     }

                     reader.AdvanceTo(buffer.End);
                  }
               }
               catch (Exception ex)
               {
                  LogServer($"Stream read error: {ex.Message}");
               }
            }
            LogServer($"Session closed: {session.Id}");
         });
      });
   })
   .Build();

LogInfo("Starting server on localhost:1337...");
await server.StartAsync();
LogSuccess("Server is running.");

INetworkSession? activeSession = null;
INetworkStream? activeStream = null;

var clientBuilder = NetworkClientBuilder.Create().UseTcp();

try
{
   while (true)
   {
      Console.ForegroundColor = ConsoleColor.DarkGray;
      Console.Write("\nCommand (c/d/s/q): ");
      Console.ResetColor();
      var input = Console.ReadLine()?.Trim().ToLower();

      if (input == "q")
      {
         LogInfo("Exiting...");
         break;
      }

      switch (input)
      {
         case "c":
            if (activeSession is not null)
            {
               LogError("Client is already connected!");
               break;
            }

            LogClient("Connecting to server...");
            var connectResult = await clientBuilder.ConnectAsync(new IPEndPoint(IPAddress.Loopback, 1337));
            if (connectResult.IsSuccess)
            {
               activeSession = connectResult.Success!;
               LogSuccess($"Client connected! Session: {activeSession.Id}");

               var streamResult = await activeSession.OpenStreamAsync();
               if (streamResult.IsSuccess)
               {
                  activeStream = streamResult.Success!;
                  LogSuccess("Client opened stream.");
               }
               else
               {
                  LogError($"Client failed to open stream: {streamResult.Error.Message}");
                  await CloseClientAsync();
               }
            }
            else
            {
               LogError($"Client connection failed: {connectResult.Error.Message}");
            }
            break;

         case "d":
            if (activeSession is null)
            {
               LogError("Client is not connected.");
               break;
            }

            await CloseClientAsync();
            break;

         case "s":
            if (activeSession is null || activeStream is null)
            {
               LogError("Client is not connected or stream is not open.");
               break;
            }

            Console.ForegroundColor = ConsoleColor.DarkGray;
            Console.Write("Enter message to send: ");
            Console.ResetColor();
            var msg = Console.ReadLine();
            if (string.IsNullOrEmpty(msg))
            {
               msg = "Hello from active Beskar client!";
            }

            try
            {
               LogClient($"Sending: \"{msg}\"");
               var writer = activeStream.Transport.Output;
               var payload = Encoding.UTF8.GetBytes(msg);
               await writer.WriteAsync(payload);
               await writer.FlushAsync();
               LogSuccess("Message sent.");
            }
            catch (Exception ex)
            {
               LogError($"Failed to send message: {ex.Message}");
            }
            break;

         default:
            LogError("Unknown command. Use c to connect, d to disconnect, s to send, or q to quit.");
            break;
      }
   }
}
finally
{
   await CloseClientAsync();
   LogInfo("Stopping server...");
   await server.StopAsync();
   LogSuccess("Server stopped.");
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
      LogClient("Disconnecting and closing session...");
      if (activeSession is IAsyncDisposable asyncDisposable)
      {
         await asyncDisposable.DisposeAsync();
      }
      else if (activeSession is IDisposable disposable)
      {
         disposable.Dispose();
      }
      activeSession = null;
      LogSuccess("Client disconnected.");
   }
}

static void LogInfo(string message)
{
   Console.ForegroundColor = ConsoleColor.Cyan;
   Console.Write("[INFO] ");
   Console.ResetColor();
   Console.WriteLine(message);
}

static void LogSuccess(string message)
{
   Console.ForegroundColor = ConsoleColor.Green;
   Console.Write("[✔] ");
   Console.ResetColor();
   Console.WriteLine(message);
}

static void LogClient(string message)
{
   Console.ForegroundColor = ConsoleColor.Blue;
   Console.Write("[CLIENT] ");
   Console.ResetColor();
   Console.WriteLine(message);
}

static void LogServer(string message)
{
   Console.ForegroundColor = ConsoleColor.Magenta;
   Console.Write("[SERVER] ");
   Console.ResetColor();
   Console.WriteLine(message);
}

static void LogError(string message)
{
   Console.ForegroundColor = ConsoleColor.Red;
   Console.Write("[ERROR] ");
   Console.ResetColor();
   Console.WriteLine(message);
}
