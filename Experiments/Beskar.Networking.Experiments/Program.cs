using Beskar.Networking.Transports.Common.Hosting;
using Beskar.Networking.Transports.Tcp;

Console.WriteLine("Hello, World!");

var server = NetworkServerBuilder.Create()
   .ConfigureServers(register =>
   {
      register.ListenLocalhost(1337, options =>
      {
         options.UseTcp();
      });
   })
   .Build();

await server.StartAsync();
