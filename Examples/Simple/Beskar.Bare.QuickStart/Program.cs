using System.Net;
using System.Text;
using Beskar.Networking.Transports.Tcp;

var endPoint = new IPEndPoint(IPAddress.Loopback, 9000);

// Server Listener
await using var listener = new TcpNetworkListener(endPoint, new TcpTransportOptions());
await listener.BindAsync();

var serverTask = Task.Run(async () =>
{
   // ReSharper disable once AccessToDisposedClosure
   var sessionResult = await listener.AcceptSessionAsync();
   if (sessionResult.Failed) return;

   await using var session = sessionResult.Success;
   var streamResult = await session.AcceptStreamAsync();
   if (streamResult.Failed) return;

   var stream = streamResult.Success;

   // Read raw pipeline input
   var readResult = await stream.Transport.Input.ReadAsync();
   Console.WriteLine($"Received: {Encoding.UTF8.GetString(readResult.Buffer.FirstSpan)}");
});

// Client Connection
await using var client = new TcpNetworkClient(new TcpTransportOptions());
var connectResult = await client.ConnectAsync(endPoint);
if (!connectResult.Failed)
{
   var session = connectResult.Success;
   var streamResult = await session.AcceptStreamAsync();
   if (!streamResult.Failed)
   {
      var clientStream = streamResult.Success;

      // Write directly to pipeline output
      var bytes = "Hello Bare Metal!"u8.ToArray();
      await clientStream.Transport.Output.WriteAsync(bytes);
   }
}

// Wait for server task to finish processing received message
await serverTask;
