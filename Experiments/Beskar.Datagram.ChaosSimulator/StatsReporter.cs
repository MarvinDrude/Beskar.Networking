using System.Diagnostics;
using Beskar.Networking.Datagram;
using Beskar.Networking.Datagram.Enums;
using Beskar.Utilities.Console.Rendering;

namespace Beskar.Datagram.ChaosSimulator;

public static class StatsReporter
{
   public static async Task RunStatsReporterAsync(DatagramHost server, CancellationToken ct)
   {
      var stopwatch = Stopwatch.StartNew();
      var prevAllocatedBytes = GC.GetTotalAllocatedBytes(precise: false);
      var prevGen0 = GC.CollectionCount(0);
      var prevGen1 = GC.CollectionCount(1);
      var prevGen2 = GC.CollectionCount(2);
      var prevPacketsSent = 0L;
      var prevPacketsRecv = 0L;
      var prevBytesSent = 0L;
      var prevBytesRecv = 0L;
      var prevElapsed = TimeSpan.Zero;

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

         PrintSnapshot(
            server,
            stopwatch.Elapsed,
            ref prevElapsed,
            ref prevAllocatedBytes,
            ref prevGen0,
            ref prevGen1,
            ref prevGen2,
            ref prevPacketsSent,
            ref prevPacketsRecv,
            ref prevBytesSent,
            ref prevBytesRecv);
      }

      // Final summary snapshot on shutdown
      PrintSnapshot(
         server,
         stopwatch.Elapsed,
         ref prevElapsed,
         ref prevAllocatedBytes,
         ref prevGen0,
         ref prevGen1,
         ref prevGen2,
         ref prevPacketsSent,
         ref prevPacketsRecv,
         ref prevBytesSent,
         ref prevBytesRecv,
         isFinal: true);
   }

   private static void PrintSnapshot(
      DatagramHost server,
      TimeSpan elapsed,
      ref TimeSpan prevElapsed,
      ref long prevAllocatedBytes,
      ref int prevGen0,
      ref int prevGen1,
      ref int prevGen2,
      ref long prevPacketsSent,
      ref long prevPacketsRecv,
      ref long prevBytesSent,
      ref long prevBytesRecv,
      bool isFinal = false)
   {
      var dtSec = Math.Max(0.001, (elapsed - prevElapsed).TotalSeconds);
      prevElapsed = elapsed;

      var totalAlloc = GC.GetTotalAllocatedBytes(precise: false);
      var deltaAlloc = totalAlloc - prevAllocatedBytes;
      prevAllocatedBytes = totalAlloc;

      var gen0 = GC.CollectionCount(0);
      var gen1 = GC.CollectionCount(1);
      var gen2 = GC.CollectionCount(2);
      var dGen0 = gen0 - prevGen0;
      var dGen1 = gen1 - prevGen1;
      var dGen2 = gen2 - prevGen2;
      prevGen0 = gen0;
      prevGen1 = gen1;
      prevGen2 = gen2;

      var managedHeapBytes = GC.GetTotalMemory(forceFullCollection: false);
      var gcInfo = GC.GetGCMemoryInfo();
      var workingSetBytes = Process.GetCurrentProcess().WorkingSet64;

      var metricPeersActive = Program.TelemetryGauges.GetValueOrDefault("beskar.datagram.peers.active", 0);
      var metricPeersConn = Program.TelemetryCounters.GetValueOrDefault("beskar.datagram.peers.connected", 0);
      var metricPeersDisc = Program.TelemetryCounters.GetValueOrDefault("beskar.datagram.peers.disconnected", 0);
      var metricPktsSent = Program.TelemetryCounters.GetValueOrDefault("beskar.datagram.packets.sent", 0);
      var metricPktsRecv = Program.TelemetryCounters.GetValueOrDefault("beskar.datagram.packets.received", 0);
      var metricRetrans = Program.TelemetryCounters.GetValueOrDefault("beskar.datagram.messages.retransmitted", 0);
      var metricSecRej = Program.TelemetryCounters.GetValueOrDefault("beskar.datagram.security.rejected", 0);
      var metricBytesSent = Program.TelemetryCounters.GetValueOrDefault("beskar.datagram.bytes.sent", 0);
      var metricBytesRecv = Program.TelemetryCounters.GetValueOrDefault("beskar.datagram.bytes.received", 0);

      var dPktsSent = metricPktsSent - prevPacketsSent;
      var dPktsRecv = metricPktsRecv - prevPacketsRecv;
      var dBytesSent = metricBytesSent - prevBytesSent;
      var dBytesRecv = metricBytesRecv - prevBytesRecv;
      prevPacketsSent = metricPktsSent;
      prevPacketsRecv = metricPktsRecv;
      prevBytesSent = metricBytesSent;
      prevBytesRecv = metricBytesRecv;

      var allocRateMbSec = deltaAlloc / dtSec / (1024.0 * 1024.0);
      var totalIntervalPkts = dPktsSent + dPktsRecv;
      var bytesPerPktText = totalIntervalPkts > 0
         ? $"~{(double)deltaAlloc / totalIntervalPkts:F0} B / wire pkt (client+server)"
         : "N/A (0 pkts)";

      // Aggregate live per-peer link quality across active server peers
      var healthy = 0;
      var congested = 0;
      var critical = 0;
      var rttSum = 0.0;
      var lossSum = 0.0;
      long totalUnacked = 0;
      var peerSampleCount = 0;

      foreach (var peer in server.Peers)
      {
         var st = peer.Stats;
         peerSampleCount++;
         rttSum += st.SmoothedRttMs;
         lossSum += st.PacketLossRate;
         totalUnacked += st.UnackedReliableMessages;
         switch (st.CongestionState)
         {
            case DatagramCongestionState.Healthy: healthy++; break;
            case DatagramCongestionState.Congested: congested++; break;
            case DatagramCongestionState.Critical: critical++; break;
         }
      }

      var avgRtt = peerSampleCount > 0 ? rttSum / peerSampleCount : 0.0;
      var avgLossPct = peerSampleCount > 0 ? (lossSum / peerSampleCount) * 100.0 : 0.0;
      var (histCount, histAvgRtt, histMaxRtt) = Program.ReadAndResetRttHistogram();
      var integrityErrors = Interlocked.Read(ref Program.DataIntegrityErrors);

      lock (Program.LogLock)
      {
         Console.WriteLine();
         Console.ForegroundColor = isFinal ? ConsoleColor.Green : ConsoleColor.Cyan;
         Console.WriteLine("==========================================================================================");
         Console.WriteLine($"  {(isFinal ? "FINAL SUMMARY" : "DATAGRAM CHAOS STATS")} | Elapsed: {elapsed:hh\\:mm\\:ss} | Profile: {Program.Profile} | Security: {Program.SecurityMode}");
         Console.WriteLine("==========================================================================================");
         Console.ResetColor();

         // 1. Connections, Lifecycle & Integrity
         ConsoleRender.CreateTable()
            .SetBorderColor(ConsoleColor.DarkCyan)
            .AddColumn("Lifecycle & Reliability", Alignment.Left, ConsoleColor.Yellow)
            .AddColumn("Count / Status", Alignment.Right, ConsoleColor.White)
            .AddColumn("Details", Alignment.Left, ConsoleColor.Gray)
            .AddRow("Active Server Peers", server.PeerCount.ToString(), $"Target Clients: {Program.TargetConcurrentClients}")
            .AddRow("Client Handshakes", Interlocked.Read(ref Program.ClientConnectSuccess).ToString(),
               $"Attempts: {Interlocked.Read(ref Program.ClientConnectAttempts)} | ExpReject: {Interlocked.Read(ref Program.ClientConnectFailExpected)} | Timeout: {Interlocked.Read(ref Program.ClientConnectTimeouts)} | UnexpFail: {Interlocked.Read(ref Program.ClientConnectFailUnexpected)}")
            .AddRow("Server Disconnects", Interlocked.Read(ref Program.ServerDisconnectsTotal).ToString(),
               $"Graceful: {Interlocked.Read(ref Program.ServerDisconnectsGraceful)} | Timeout: {Interlocked.Read(ref Program.ServerDisconnectsTimeout)} | MaxRetries: {Interlocked.Read(ref Program.ServerDisconnectsMaxRetries)}")
            .AddRow("NAT Migrations", Interlocked.Read(ref Program.ServerMigrationsObserved).ToString(),
               $"Client Triggered: {Interlocked.Read(ref Program.ClientMigrationsTriggered)} | Fuzz Sent: {Interlocked.Read(ref Program.ClientFuzzPacketsSent)}")
            .AddRow("Peer Congestion States", $"{avgRtt:F1} ms avg RTT",
               $"Healthy: {healthy} | Congested: {congested} | Critical: {critical} | AvgLoss: {avgLossPct:F1}% | Unacked: {totalUnacked}")
            .AddRow("Data Integrity Errors", integrityErrors.ToString(),
               integrityErrors == 0 ? "PASS (0 corrupt / 0 out-of-order Ch0)" : "FAIL - ORDER OR CHECKSUM VIOLATION!")
            .Render();

         // 2. Per-Channel Message Breakdown (All 5 Delivery Modes)
         ConsoleRender.CreateTable()
            .SetBorderColor(ConsoleColor.DarkGreen)
            .AddColumn("Channel & Delivery Mode", Alignment.Left, ConsoleColor.Yellow)
            .AddColumn("Clients Sent", Alignment.Right, ConsoleColor.White)
            .AddColumn("Server Received", Alignment.Right, ConsoleColor.White)
            .AddColumn("Notes", Alignment.Left, ConsoleColor.Gray)
            .AddRow("Ch 0 (ReliableOrdered)", Interlocked.Read(ref Program.ClientSentCh0Ordered).ToString("N0"),
               Interlocked.Read(ref Program.ServerRecvCh0Ordered).ToString("N0"),
               $"Strict FIFO + CRC verified | Frag Sent: {Interlocked.Read(ref Program.ClientFragmentsSent):N0}")
            .AddRow("Ch 1 (ReliableUnordered)", Interlocked.Read(ref Program.ClientSentCh1Unordered).ToString("N0"),
               Interlocked.Read(ref Program.ServerRecvCh1Unordered).ToString("N0"),
               "No HoL stall + CRC verified")
            .AddRow("Ch 2 (UnreliableSequenced)", Interlocked.Read(ref Program.ClientSentCh2UnrelSeq).ToString("N0"),
               Interlocked.Read(ref Program.ServerRecvCh2UnrelSeq).ToString("N0"),
               "Drop-stale snapshots/inputs")
            .AddRow("Ch 3 (UnreliableUnordered)", Interlocked.Read(ref Program.ClientSentCh3UnrelUnord).ToString("N0"),
               Interlocked.Read(ref Program.ServerRecvCh3UnrelUnord).ToString("N0"),
               "Fire-and-forget FX cues")
            .AddRow("Ch 4 (ReliableSequenced)", Interlocked.Read(ref Program.ClientSentCh4RelSeq).ToString("N0"),
               Interlocked.Read(ref Program.ServerRecvCh4RelSeq).ToString("N0"),
               "Latest-only reliable convergence")
            .AddRow("Server -> Client Broadcasts", Interlocked.Read(ref Program.ServerBroadcastsSent).ToString("N0"),
               Interlocked.Read(ref Program.ClientMessagesReceived).ToString("N0"),
               $"QueueFull Backpressure: {Interlocked.Read(ref Program.ClientSendQueueFull):N0}")
            .Render();

         // 3. Memory, Allocations & GC Over Time
         ConsoleRender.CreateTable()
            .SetBorderColor(ConsoleColor.DarkYellow)
            .AddColumn("Memory & GC Metric", Alignment.Left, ConsoleColor.Yellow)
            .AddColumn("Live Value", Alignment.Right, ConsoleColor.White)
            .AddColumn("Interval Delta / Detail", Alignment.Left, ConsoleColor.Gray)
            .AddRow("Managed Heap Size", $"{managedHeapBytes / (1024.0 * 1024.0):F2} MB",
               $"Working Set: {workingSetBytes / (1024.0 * 1024.0):F2} MB | GC Heap: {gcInfo.HeapSizeBytes / (1024.0 * 1024.0):F2} MB")
            .AddRow("Allocation Rate", $"{allocRateMbSec:F2} MB/s",
               $"Total Allocated: {totalAlloc / (1024.0 * 1024.0):F1} MB | {bytesPerPktText}")
            .AddRow("GC Collections (G0/G1/G2)", $"{gen0} / {gen1} / {gen2}",
               $"Interval Delta: +{dGen0} / +{dGen1} / +{dGen2} | Pinned: {gcInfo.PinnedObjectsCount}")
            .AddRow("Throughput (Interval)", $"{(dPktsSent + dPktsRecv) / dtSec:N0} pkts/s",
               $"TX: {dBytesSent / dtSec / (1024.0 * 1024.0):F2} MB/s | RX: {dBytesRecv / dtSec / (1024.0 * 1024.0):F2} MB/s")
            .Render();

         // 4. OpenTelemetry System.Diagnostics.Metrics Table
         ConsoleRender.CreateTable()
            .SetBorderColor(ConsoleColor.Magenta)
            .AddColumn("Meter", Alignment.Left, ConsoleColor.Magenta)
            .AddColumn("Instrument", Alignment.Left, ConsoleColor.Yellow)
            .AddColumn("Type / Unit", Alignment.Left, ConsoleColor.Cyan)
            .AddColumn("Live Value", Alignment.Right, ConsoleColor.White)
            .AddRow("Beskar.Networking.Datagram", "beskar.datagram.peers.active", "UpDownCounter {peer}", metricPeersActive.ToString("N0"))
            .AddRow("Beskar.Networking.Datagram", "beskar.datagram.peers.connected/disconnected", "Counter {peer}", $"Conn: {metricPeersConn:N0} | Disc: {metricPeersDisc:N0}")
            .AddRow("Beskar.Networking.Datagram", "beskar.datagram.packets.sent/received", "Counter {packet}", $"Sent: {metricPktsSent:N0} | Recv: {metricPktsRecv:N0}")
            .AddRow("Beskar.Networking.Datagram", "beskar.datagram.messages.retransmitted", "Counter {message}", metricRetrans.ToString("N0"))
            .AddRow("Beskar.Networking.Datagram", "beskar.datagram.security.rejected", "Counter {packet}", metricSecRej.ToString("N0"))
            .AddRow("Beskar.Networking.Datagram", "beskar.datagram.bytes.sent/received", "Counter By", $"Sent: {metricBytesSent:N0} | Recv: {metricBytesRecv:N0}")
            .AddRow("Beskar.Networking.Datagram", "beskar.datagram.rtt", "Histogram ms", histCount > 0 ? $"Avg: {histAvgRtt:F1}ms | Max: {histMaxRtt:F1}ms ({histCount:N0} samples)" : "N/A")
            .Render();

         Console.ForegroundColor = ConsoleColor.Cyan;
         Console.WriteLine("==========================================================================================");
         Console.ResetColor();
      }
   }
}
