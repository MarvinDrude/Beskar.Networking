using System.Buffers;
using System.Net;
using System.Text;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Common.Hosting;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Tcp.Extensions;
using Beskar.Utilities.Console.Rendering;
using Beskar.Utilities.Tracing;

Console.Clear();
ConsoleRender.DrawHeader(
    "BESKAR NETWORKING EXPERIMENTS",
    "Automated TCP Client/Server Connection Demo",
    BoxStyle.Rounded,
    ConsoleColor.Yellow
);

const int Port = 1337;
TraceLogger.IsEnabled = true;

var server = NetworkServerBuilder.Create()
   .ConfigureServers(register =>
   {
      register.ListenLocalhost(Port, options =>
      {
         options.UseTcp();
         options.OnSession(async session =>
         {
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
                     }

                     reader.AdvanceTo(buffer.End);
                  }
               }
               catch (Exception ex)
               {
                  ConsoleRender.Server($"Stream read error: {ex.Message}");
               }
            }
         });
      });
   })
   .Build();

var clientBuilder = NetworkClientBuilder.Create().UseTcp();

try
{
   await server.StartAsync();
   await Task.Delay(200);

   INetworkSession? activeSession;
   INetworkStream? activeStream;

   var connectResult = await clientBuilder.ConnectAsync(new IPEndPoint(IPAddress.Loopback, Port));
   if (connectResult.IsSuccess)
   {
      activeSession = connectResult.Success!;
      var streamResult = await activeSession.OpenStreamAsync();
      if (streamResult.IsSuccess)
      {
         activeStream = streamResult.Success!;
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

   if (true)
   {
      const string msg = "Hello from automated Beskar client!";
      var writer = activeStream.Transport.Output;
      var payload = Encoding.UTF8.GetBytes(msg);

      await writer.WriteAsync(payload);
      await writer.FlushAsync();

      await Task.Delay(400);

      await activeStream.DisposeAsync();
      if (activeSession is IAsyncDisposable asyncDisposable)
      {
         await asyncDisposable.DisposeAsync();
      }
      await Task.Delay(200);
   }
}
catch (Exception ex)
{
   ConsoleRender.Error($"Demo error: {ex.Message}");
}
finally
{
   await server.StopAsync();
   await Task.Delay(200);
}
