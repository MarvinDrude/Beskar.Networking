using System.Diagnostics;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Quic;
using Beskar.Networking.Transports.Tcp;
using Beskar.Networking.Transports.Ws;

namespace Beskar.Networking.RealWorldWavesClient;

public static class Program
{
   public static async Task Main(string[] args)
   {
      string transportName = args.Length > 0 ? args[0].ToUpperInvariant() : "QUIC";
      int waveCount = args.Length > 1 && int.TryParse(args[1], out var w) ? w : 20;
      int usersPerWave = args.Length > 2 && int.TryParse(args[2], out var users) ? users : 50;
      int messagesPerUser = args.Length > 3 && int.TryParse(args[3], out var m) ? m : 10;
      int waveDelayMs = args.Length > 4 && int.TryParse(args[4], out var d) ? d : 1500;
      int port = args.Length > 5 && int.TryParse(args[5], out var p) ? p : 9550;

      Console.ForegroundColor = ConsoleColor.Cyan;
      Console.WriteLine("╔═══════════════════════════════════════════════════════════════════════════════════════╗");
      Console.WriteLine("║                                                                                       ║");
      Console.WriteLine("║            BESKAR REAL-WORLD CLIENT-POOLED USER WAVES BENCHMARK                       ║");
      Console.WriteLine("║     Dedicated Client Process: Reuses Pooled Client Instances Across Waves             ║");
      Console.WriteLine("║                                                                                       ║");
      Console.WriteLine("╚═══════════════════════════════════════════════════════════════════════════════════════╝");
      Console.ResetColor();
      Console.WriteLine();

      Console.WriteLine($"Transport Selected      : {transportName}");
      Console.WriteLine($"Target Server Endpoint  : 127.0.0.1:{port}");
      Console.WriteLine($"Total Waves             : {waveCount}");
      Console.WriteLine($"Users Per Wave          : {usersPerWave}");
      Console.WriteLine($"Messages Per User Stream: {messagesPerUser}");
      Console.WriteLine($"Delay Between Waves     : {waveDelayMs} ms");
      Console.WriteLine($"Total Distinct Users    : {waveCount * usersPerWave}");
      Console.WriteLine();

      var endPoint = new IPEndPoint(IPAddress.Loopback, port);

      // Pre-create pooled client instances for the batch size
      var clientPool = new INetworkClient[usersPerWave];
      for (int i = 0; i < usersPerWave; i++)
      {
         if (transportName == "QUIC")
         {
            var quicOptions = new QuicTransportOptions
            {
               IdleTimeout = TimeSpan.FromSeconds(30),
               HandshakeTimeout = TimeSpan.FromSeconds(5),
               SslClientOptions = new SslClientAuthenticationOptions
               {
                  TargetHost = "localhost",
                  RemoteCertificateValidationCallback = (_, _, _, _) => true
               }
            };
            clientPool[i] = new QuicNetworkClient(quicOptions);
         }
         else if (transportName == "TCP")
         {
            clientPool[i] = new TcpNetworkClient(new TcpTransportOptions());
         }
         else if (transportName == "WS")
         {
            clientPool[i] = new WsNetworkClient(new WsTransportOptions());
         }
      }

      // Measure baseline memory before waves start
      GC.Collect();
      GC.WaitForPendingFinalizers();
      GC.Collect();

      var initialMemoryMB = Process.GetCurrentProcess().PrivateMemorySize64 / 1024.0 / 1024.0;
      var initialGcMB = GC.GetTotalMemory(true) / 1024.0 / 1024.0;

      Console.ForegroundColor = ConsoleColor.Yellow;
      Console.WriteLine($"Baseline Client Process Memory: {initialMemoryMB:F1} MB (GC Managed: {initialGcMB:F1} MB)");
      Console.ResetColor();
      Console.WriteLine();

      Console.WriteLine("┌──────────┬─────────────────┬────────────────────┬─────────────────────┬──────────────────┬─────────────────┐");
      Console.WriteLine("│ Wave #   │ Total Users     │ Active Connections │ Client Memory (MB)  │ GC Managed (MB)  │ Memory Delta    │");
      Console.WriteLine("├──────────┼─────────────────┼────────────────────┼─────────────────────┼──────────────────┼─────────────────┤");

      long totalUsersCount = 0;

      for (int wave = 1; wave <= waveCount; wave++)
      {
         var userTasks = new List<Task>();
         for (int userIdx = 0; userIdx < usersPerWave; userIdx++)
         {
            int userId = (wave - 1) * usersPerWave + userIdx + 1;
            var client = clientPool[userIdx];
            userTasks.Add(SimulateSingleUserWithPooledClientAsync(client, endPoint, userId, messagesPerUser));
         }

         await Task.WhenAll(userTasks);
         totalUsersCount += usersPerWave;

         // Cooldown between waves
         await Task.Delay(waveDelayMs);

         GC.Collect(2, GCCollectionMode.Forced, true, true);

         var currentProcessMB = Process.GetCurrentProcess().PrivateMemorySize64 / 1024.0 / 1024.0;
         var currentGcMB = GC.GetTotalMemory(false) / 1024.0 / 1024.0;
         var deltaMB = currentProcessMB - initialMemoryMB;

         string deltaFormatted = deltaMB >= 0 ? $"+{deltaMB:F1} MB" : $"{deltaMB:F1} MB";

         Console.WriteLine($"│ Wave {wave:D2}/{waveCount:D2} │ {totalUsersCount,15:N0} │ {0,18:N0} │ {currentProcessMB,19:F1} │ {currentGcMB,16:F1} │ {deltaFormatted,15} │");
      }

      Console.WriteLine("└──────────┴─────────────────┴────────────────────┴─────────────────────┴──────────────────┴─────────────────┘");
      Console.WriteLine();

      // Clean up pooled clients
      foreach (var client in clientPool)
      {
         await client.DisposeAsync();
      }

      GC.Collect();
      GC.WaitForPendingFinalizers();
      GC.Collect();

      var finalMemoryMB = Process.GetCurrentProcess().PrivateMemorySize64 / 1024.0 / 1024.0;

      Console.ForegroundColor = ConsoleColor.Green;
      Console.WriteLine($"Client Benchmark Complete! Final Client Process Memory: {finalMemoryMB:F1} MB (Peak Delta: +{(finalMemoryMB - initialMemoryMB):F1} MB)");
      Console.ResetColor();
   }

   private static async Task SimulateSingleUserWithPooledClientAsync(
      INetworkClient client, EndPoint endPoint, int userId, int messageCount)
   {
      try
      {
         var connectResult = await client.ConnectAsync(endPoint);
         if (connectResult.Failed || connectResult.Success is null) return;

         var session = connectResult.Success;
         var streamResult = await session.OpenStreamAsync();
         if (streamResult.Failed || streamResult.Success is null) return;

         var stream = streamResult.Success;
         await using (stream)
         {
            var payload = "User Message Sample Data Payload "u8.ToArray();

            for (int m = 0; m < messageCount; m++)
            {
               await stream.Transport.Output.WriteAsync(payload);
               await stream.Transport.Output.FlushAsync();

               var readResult = await stream.Transport.Input.ReadAsync();
               if (readResult.Buffer.IsEmpty && readResult.IsCompleted) break;

               stream.Transport.Input.AdvanceTo(readResult.Buffer.End);
            }
         }

         await client.DisconnectAsync();
      }
      catch
      {
         // Ignored
      }
   }
}
