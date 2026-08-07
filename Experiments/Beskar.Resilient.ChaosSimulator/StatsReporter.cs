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
               .AddRow("Shared Memory Blocks",
                  $"Rented: {Beskar.Networking.Transports.Common.Options.SharedTransportMemoryPool.GetStats().Rented} | InStore: {Beskar.Networking.Transports.Common.Options.SharedTransportMemoryPool.GetStats().InStore} | Total: {Beskar.Networking.Transports.Common.Options.SharedTransportMemoryPool.GetStats().Created}")
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

            // Render Live System.Diagnostics.Metrics OpenTelemetry table
            var transportActiveConns = Program.TelemetryGauges.GetValueOrDefault("beskar.transport.connections.active", 0);
            var transportOpenedConns = Program.TelemetryCounters.GetValueOrDefault("beskar.transport.connections.opened", 0);
            var transportClosedConns = Program.TelemetryCounters.GetValueOrDefault("beskar.transport.connections.closed", 0);
            var transportBytesSent = Program.TelemetryCounters.GetValueOrDefault("beskar.transport.bytes.sent", 0);
            var transportBytesRecv = Program.TelemetryCounters.GetValueOrDefault("beskar.transport.bytes.received", 0);

            var resilientActiveSessions = Program.TelemetryGauges.GetValueOrDefault("beskar.resilient.sessions.active", 0);
            var resilientReconnectAttempts = Program.TelemetryCounters.GetValueOrDefault("beskar.resilient.reconnect.attempts", 0);
            var resilientAuthAttempts = Program.TelemetryCounters.GetValueOrDefault("beskar.resilient.auth.attempts", 0);
            var resilientOfflineQueueSize = Program.TelemetryGauges.GetValueOrDefault("beskar.resilient.offline_queue.size", 0);
            var resilientOfflineQueueDropped = Program.TelemetryCounters.GetValueOrDefault("beskar.resilient.offline_queue.dropped", 0);

            ConsoleRender.CreateTable()
               .SetBorderColor(ConsoleColor.Magenta)
               .AddColumn("OpenTelemetry Meter", Alignment.Left, ConsoleColor.Magenta)
               .AddColumn("Instrument Name", Alignment.Left, ConsoleColor.Yellow)
               .AddColumn("Type / Unit", Alignment.Left, ConsoleColor.Cyan)
               .AddColumn("Live Value", Alignment.Right, ConsoleColor.White)
               .AddRow("Beskar.Networking.Transport", "beskar.transport.connections.active", "UpDownCounter {connection}", transportActiveConns.ToString())
               .AddRow("Beskar.Networking.Transport", "beskar.transport.connections.opened/closed", "Counter {connection}", $"Opened: {transportOpenedConns} | Closed: {transportClosedConns}")
               .AddRow("Beskar.Networking.Transport", "beskar.transport.bytes.sent/received", "Counter By", $"Sent: {transportBytesSent:N0} B | Recv: {transportBytesRecv:N0} B")
               .AddRow("Beskar.Networking.Resilient", "beskar.resilient.sessions.active", "UpDownCounter {session}", resilientActiveSessions.ToString())
               .AddRow("Beskar.Networking.Resilient", "beskar.resilient.reconnect.attempts", "Counter {attempt}", resilientReconnectAttempts.ToString())
               .AddRow("Beskar.Networking.Resilient", "beskar.resilient.auth.attempts", "Counter {attempt}", resilientAuthAttempts.ToString())
               .AddRow("Beskar.Networking.Resilient", "beskar.resilient.offline_queue.size", "UpDownCounter {message}", resilientOfflineQueueSize.ToString())
               .AddRow("Beskar.Networking.Resilient", "beskar.resilient.offline_queue.dropped", "Counter {message}", resilientOfflineQueueDropped.ToString())
               .Render();

            Console.ForegroundColor = ConsoleColor.Cyan;
            Console.WriteLine("==================================================================================");
            Console.ResetColor();
            Console.WriteLine();
         }
      }
   }
}
