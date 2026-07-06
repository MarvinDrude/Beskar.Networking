using System;
using System.Reflection;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Transports.Common.Hosting;
using Beskar.Networking.Transports.Tcp.Extensions;
using Beskar.Utilities.Tracing;

TraceLogger.IsEnabled = true;
Console.WriteLine();

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

