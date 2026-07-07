using System;
using System.Reflection;
using Beskar.Memory.Results;
using Beskar.Mqtt.Server;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Transports.Common.Hosting;
using Beskar.Networking.Transports.Tcp.Extensions;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;
Console.WriteLine();

var mqttServer = MqttServerFactory.CreateBuilder()
   .UseTcp(8000)
   .UseWs(8001)
   .UseQuic(8002)
   .Build();



var server = NetworkServerBuilder.Create()
   .ConfigureServers(collection =>
   {
      collection.ListenAnyIP(9000, builder =>
      {
         builder.UseTcp(tcpBuilder =>
         {

         });
         builder.OnSession(static session =>
         {
            return Task.CompletedTask;
         });
      });
   })
   .Build();

