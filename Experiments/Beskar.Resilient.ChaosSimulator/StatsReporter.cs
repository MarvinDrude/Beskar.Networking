using System.Diagnostics;
using Beskar.Utilities.Console.Rendering;

namespace Beskar.Resilient.ChaosSimulator;

public static class StatsReporter
{
   public static async Task RunStatsReporterAsync(CancellationToken ct)
   {
      var stopwatch = Stopwatch.StartNew();

      while (!ct.IsCancellationRequested)
      {
         try
         {
            await Task.Delay(Program.StatsIntervalSeconds * 1000, ct);
         }
         catch (OperationCanceledException)
         {
            break;
         }

         lock (Program.LogLock)
         {
            var elapsed = stopwatch.Elapsed;
            Console.WriteLine();
            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("==================================================================================");
            Console.WriteLine($"           RESILIENT STATS REPORT (Running Time: {elapsed:hh\\:mm\\:ss})");
            Console.WriteLine("==================================================================================");
            Console.ResetColor();

            // Render Connection stats table
            ConsoleRender.CreateTable()
               .SetBorderColor(ConsoleColor.DarkCyan)
               .AddColumn("Metric Type", Alignment.Left, ConsoleColor.Yellow)
               .AddColumn("Details / Status", Alignment.Left, ConsoleColor.Gray)
               .AddRow("Active Transport Connections",
                  $"TCP: {Interlocked.Read(ref Program.ActiveTcpConnections)} | WS: {Interlocked.Read(ref Program.ActiveWsConnections)} | QUIC: {Interlocked.Read(ref Program.ActiveQuicConnections)}")
               .AddRow("Server Disconnect Tracking",
                  $"Graceful: {Interlocked.Read(ref Program.ServerConnectionsGraceful)} | Abrupt/Crash: {Interlocked.Read(ref Program.ServerConnectionsAbrupt)}")
               .Render();

            // Render Client operations stats table
            ConsoleRender.CreateTable()
               .SetBorderColor(ConsoleColor.DarkGreen)
               .AddColumn("Operation Details", Alignment.Left, ConsoleColor.Yellow)
               .AddColumn("Server Side", Alignment.Right, ConsoleColor.White)
               .AddColumn("Client Side", Alignment.Right, ConsoleColor.White)
               .AddRow("Connect Attempts", Program.ServerConnectionsTotal.ToString(), Program.ClientAttempts.ToString())
               .AddRow("Successful Handshakes", Program.ServerConnectionsTotal.ToString(), Program.ClientConnectSuccess.ToString())
               .AddRow("Failed Connections", "", Program.ClientConnectFailUnexpected.ToString())
               .AddRow("Messages Sent/Received", Program.ServerMessagesTotal.ToString(), Program.ClientMessagesSent.ToString())
               .AddRow("Messages Received on Clients", "", Program.ClientMessagesReceived.ToString())
               .AddRow("KeepAlive/Ping Transmitted", "", Program.ClientPingsSent.ToString())
               .Render();

            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("==================================================================================");
            Console.ResetColor();
            Console.WriteLine();
         }
      }
   }
}
