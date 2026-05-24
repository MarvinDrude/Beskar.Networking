using System.Buffers;
using System.Net;
using System.Text;
using Beskar.Networking.Transports.Common.Hosting;
using Beskar.Networking.Transports.Tcp;

var server = NetworkServerBuilder.Create()
   .ConfigureServers(register =>
   {
      register.ListenLocalhost(1337, options =>
      {
         options.UseTcp();
         options.OnSession(async session =>
         {
            Console.WriteLine($"Server: Session accepted {session.Id}");
            var streamResult = await session.AcceptStreamAsync();

            if (streamResult.IsSuccess)
            {
               var stream = streamResult.Success!;
               var reader = stream.Transport.Input;
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
                     Console.WriteLine($"Server received: {message}");
                  }

                  reader.AdvanceTo(buffer.End);
               }
            }
         });
      });
   })
   .Build();

await server.StartAsync();

var clientResult = await NetworkClientBuilder.Create()
   .UseTcp()
   .ConnectAsync(new IPEndPoint(IPAddress.Loopback, 1337));

if (clientResult.IsSuccess)
{
   var session = clientResult.Success!;
   Console.WriteLine($"Client: Connected with Session {session.Id}");

   var streamResult = await session.OpenStreamAsync();
   if (streamResult.IsSuccess)
   {
      var stream = streamResult.Success!;
      var writer = stream.Transport.Output;
      var payload = "Hello from Beskar Client Builder!"u8.ToArray();

      await writer.WriteAsync(payload);
      await writer.FlushAsync();
      await stream.DisposeAsync();
   }

   if (session is IAsyncDisposable asyncDisposable)
   {
      await asyncDisposable.DisposeAsync();
   }
}

await Task.Delay(1000);
await server.StopAsync();
