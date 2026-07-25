using System.Diagnostics;
using Beskar.Utilities.Console.Rendering;

namespace Beskar.Mqtt.ChaosSimulator;

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
            Console.WriteLine($"               STATS REPORT (Running Time: {elapsed:hh\\:mm\\:ss})");
            Console.WriteLine("==================================================================================");
            Console.ResetColor();

            // Render Connection stats table
            ConsoleRender.CreateTable()
               .SetBorderColor(ConsoleColor.DarkCyan)
               .AddColumn("Metric Type", Alignment.Left, ConsoleColor.Yellow)
               .AddColumn("V3 Count", Alignment.Right, ConsoleColor.White)
               .AddColumn("V5 Count", Alignment.Right, ConsoleColor.White)
               .AddColumn("Total / Details", Alignment.Left, ConsoleColor.Gray)
               .AddRow("Client Auth Successes", Program.ServerAuthV3Success.ToString(),
                  Program.ServerAuthV5Success.ToString(),
                  $"Total: {Program.ServerAuthV3Success + Program.ServerAuthV5Success}")
               .AddRow("Client Auth Failures", Program.ServerAuthV3Failure.ToString(),
                  Program.ServerAuthV5Failure.ToString(),
                  $"Total: {Program.ServerAuthV3Failure + Program.ServerAuthV5Failure}")
               .AddRow("Active Connections", "", "",
                  $"TCP: {Interlocked.Read(ref Program.ActiveTcpConnections)} | WS: {Interlocked.Read(ref Program.ActiveWsConnections)} | QUIC: {Interlocked.Read(ref Program.ActiveQuicConnections)}")
               .AddRow("Server Disconnects", "", "",
                  $"Graceful: {Interlocked.Read(ref Program.ServerConnectionsGraceful)} | Abrupt/Crash: {Interlocked.Read(ref Program.ServerConnectionsAbrupt)}")
               .AddRow("Shared Memory Blocks", "", "",
                  $"Rented: {Beskar.Networking.Transports.Common.Options.SharedTransportMemoryPool.GetStats().Rented} | InStore: {Beskar.Networking.Transports.Common.Options.SharedTransportMemoryPool.GetStats().InStore} | Total: {Beskar.Networking.Transports.Common.Options.SharedTransportMemoryPool.GetStats().Created}")
               .Render();

            // Render Client operations stats table
            ConsoleRender.CreateTable()
               .SetBorderColor(ConsoleColor.DarkGreen)
               .AddColumn("Operation Details", Alignment.Left, ConsoleColor.Yellow)
               .AddColumn("Server Received", Alignment.Right, ConsoleColor.White)
               .AddColumn("Clients Executed", Alignment.Right, ConsoleColor.White)
               .AddRow("Connect Attempts", Program.ServerConnectionsTotal.ToString(), Program.ClientAttempts.ToString())
               .AddRow("Successful Handshakes", (Program.ServerAuthV3Success + Program.ServerAuthV5Success).ToString(),
                  Program.ClientConnectSuccess.ToString())
               .AddRow("Expected Failures", (Program.ServerAuthV3Failure + Program.ServerAuthV5Failure).ToString(),
                  Program.ClientConnectFailExpected.ToString())
               .AddRow("Unexpected Failures", "", Program.ClientConnectFailUnexpected.ToString())
               .AddRow("Publishes QoS 0", Program.ServerPublishesQoS0.ToString(), "")
               .AddRow("Publishes QoS 1", Program.ServerPublishesQoS1.ToString(), "")
               .AddRow("Publishes QoS 2", Program.ServerPublishesQoS2.ToString(), "")
               .AddRow("Publishes Total", Program.ServerPublishesTotal.ToString(),
                  Program.ClientPublishesSent.ToString())
               .AddRow("Publish Failures", "", Program.ClientPublishesFailed.ToString())
               .AddRow("Messages Received", "", Program.ClientMessagesReceived.ToString())
               .AddRow("No Subscriber Publishes", Program.ServerNoSubscriberMessages.ToString(), "")
               .AddRow("Subscriptions Active", Program.ServerSubscriptions.ToString(), "")
               .AddRow("Unsubscriptions Active", Program.ServerUnsubscriptions.ToString(), "")
               .AddRow("KeepAlive Pings", "", Program.ClientPingsSent.ToString())
               .Render();

            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("==================================================================================");
            Console.ResetColor();
            Console.WriteLine();
         }
      }
   }
}
