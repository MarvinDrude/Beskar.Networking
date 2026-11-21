using System.Buffers.Binary;
using System.Net;
using System.Text;
using Beskar.Networking.Datagram;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Datagram.ChaosSimulator;

public static class ClientBehaviors
{
   /// <summary>
   /// Encodes a verifiable ReliableOrdered frame: [Seq (4B)] [Checksum (4B)] [Body...]
   /// </summary>
   public static void WriteVerifiablePayload(Span<byte> dest, int seq, int payloadLength)
   {
      BinaryPrimitives.WriteInt32LittleEndian(dest[0..4], seq);

      var body = dest[8..payloadLength];
      for (var i = 0; i < body.Length; i++)
      {
         body[i] = (byte)((seq * 31 + i * 17) & 0xFF);
      }

      var checksum = ComputeQuickHash(dest[0..4], body);
      BinaryPrimitives.WriteUInt32LittleEndian(dest[4..8], checksum);
   }

   public static bool VerifyPayload(ReadOnlySpan<byte> payload, out int seq)
   {
      seq = 0;
      if (payload.Length < 8) return false;

      seq = BinaryPrimitives.ReadInt32LittleEndian(payload[0..4]);
      var expected = BinaryPrimitives.ReadUInt32LittleEndian(payload[4..8]);
      var actual = ComputeQuickHash(payload[0..4], payload[8..]);
      return actual == expected;
   }

   private static uint ComputeQuickHash(ReadOnlySpan<byte> header, ReadOnlySpan<byte> body)
   {
      var h = 2166136261u;
      foreach (var b in header) { h ^= b; h *= 16777619u; }
      foreach (var b in body) { h ^= b; h *= 16777619u; }
      return h;
   }

   public static async Task ExecuteActionPlayerAsync(
      DatagramPeer peer,
      int durationMs,
      CancellationToken ct)
   {
      var deadline = Environment.TickCount64 + durationMs;
      var orderedSeq = 0;
      var snapshotSeq = 0;
      var hudSeq = 0;
      var smallBuf = new byte[48];
      var snapshotBuf = new byte[32];

      while (Environment.TickCount64 < deadline && !ct.IsCancellationRequested && peer.State == DatagramPeerState.Connected)
      {
         BinaryPrimitives.WriteInt32LittleEndian(snapshotBuf, ++snapshotSeq);
         RecordSendResult(peer.TrySend(2, snapshotBuf), channelId: 2);

         if ((snapshotSeq & 3) == 0)
         {
            RecordSendResult(peer.TrySend(3, snapshotBuf.AsSpan(0, 16)), channelId: 3);
         }

         if (snapshotSeq % 5 == 0)
         {
            WriteVerifiablePayload(smallBuf, snapshotSeq, smallBuf.Length);
            RecordSendResult(peer.TrySend(1, smallBuf), channelId: 1);
         }

         if (snapshotSeq % 8 == 0)
         {
            WriteVerifiablePayload(smallBuf, orderedSeq++, smallBuf.Length);
            var res = peer.TrySend(0, smallBuf);
            RecordSendResult(res, channelId: 0);
            if (res != DatagramSendResult.Queued)
            {
               orderedSeq--; // Keep sequence contiguous if queue was temporarily full
            }
         }

         if (snapshotSeq % 10 == 0)
         {
            BinaryPrimitives.WriteInt32LittleEndian(snapshotBuf, ++hudSeq);
            RecordSendResult(peer.TrySend(4, snapshotBuf.AsSpan(0, 12)), channelId: 4);
         }

         var delayMs = Program.Profile == SimulatorProfile.HighThroughputSoak ? 16 : Random.Shared.Next(16, 40);
         await Task.Delay(delayMs, ct);
      }
   }

   public static async Task ExecuteStateSyncFragmenterAsync(
      DatagramPeer peer,
      int durationMs,
      CancellationToken ct)
   {
      var deadline = Environment.TickCount64 + durationMs;
      var orderedSeq = 0;
      var unorderedSeq = 0;
      var bigBuf = new byte[16_384];

      while (Environment.TickCount64 < deadline && !ct.IsCancellationRequested && peer.State == DatagramPeerState.Connected)
      {
         var len = Random.Shared.Next(2_600, 14_000);

         if ((orderedSeq + unorderedSeq) % 2 == 0)
         {
            WriteVerifiablePayload(bigBuf.AsSpan(0, len), orderedSeq, len);
            var res = peer.TrySend(0, bigBuf.AsSpan(0, len));
            RecordSendResult(res, channelId: 0);
            if (res == DatagramSendResult.Queued)
            {
               orderedSeq++;
               Interlocked.Increment(ref Program.ClientFragmentsSent);
            }
         }
         else
         {
            WriteVerifiablePayload(bigBuf.AsSpan(0, len), ++unorderedSeq, len);
            var res = peer.TrySend(1, bigBuf.AsSpan(0, len));
            RecordSendResult(res, channelId: 1);
            if (res == DatagramSendResult.Queued)
            {
               Interlocked.Increment(ref Program.ClientFragmentsSent);
            }
         }

         await Task.Delay(Random.Shared.Next(120, 350), ct);
      }
   }

   public static async Task ExecuteRoamingMobileAsync(
      DatagramPeer peer,
      SimulatedNetworkHub? hub,
      SimulatedDatagramTransport? simTransport,
      int durationMs,
      CancellationToken ct)
   {
      var deadline = Environment.TickCount64 + durationMs;
      var orderedSeq = 0;
      var tick = 0;
      var buf = new byte[40];

      while (Environment.TickCount64 < deadline && !ct.IsCancellationRequested && peer.State == DatagramPeerState.Connected)
      {
         tick++;
         WriteVerifiablePayload(buf, orderedSeq, buf.Length);
         var res = peer.TrySend(0, buf);
         RecordSendResult(res, channelId: 0);
         if (res == DatagramSendResult.Queued) orderedSeq++;

         BinaryPrimitives.WriteInt32LittleEndian(buf, tick);
         RecordSendResult(peer.TrySend(2, buf.AsSpan(0, 16)), channelId: 2);

         if (hub is not null && simTransport is not null && tick % 25 == 0)
         {
            try
            {
               var ipByte = (byte)Random.Shared.Next(10, 220);
               var newPort = Random.Shared.Next(20000, 62000);
               hub.MigrateEndpoint(simTransport, new IPEndPoint(IPAddress.Parse($"10.42.{ipByte}.1"), newPort));
               Interlocked.Increment(ref Program.ClientMigrationsTriggered);
            }
            catch (InvalidOperationException)
            {
            }
         }

         await Task.Delay(Random.Shared.Next(25, 60), ct);
      }
   }

   public static async Task ExecuteFlakyCrasherAsync(
      DatagramPeer peer,
      CancellationToken ct)
   {
      var buf = new byte[3_200];
      for (var i = 0; i < 4 && !ct.IsCancellationRequested && peer.State == DatagramPeerState.Connected; i++)
      {
         WriteVerifiablePayload(buf, i, buf.Length);
         RecordSendResult(peer.TrySend(0, buf), channelId: 0);
         RecordSendResult(peer.TrySend(2, buf.AsSpan(0, 24)), channelId: 2);
         await Task.Delay(Random.Shared.Next(20, 80), ct);
      }
   }

   public static async Task ExecuteKeepAliveIdlerAsync(
      DatagramPeer peer,
      int durationMs,
      CancellationToken ct)
   {
      var deadline = Environment.TickCount64 + durationMs;
      while (Environment.TickCount64 < deadline && !ct.IsCancellationRequested && peer.State == DatagramPeerState.Connected)
      {
         await Task.Delay(250, ct);
      }

      if (peer.State == DatagramPeerState.Connected)
      {
         var buf = new byte[32];
         WriteVerifiablePayload(buf, 0, buf.Length);
         RecordSendResult(peer.TrySend(0, buf), channelId: 0);
         await Task.Delay(100, ct);
      }
   }

   public static async Task ExecuteAuthRejectOrFuzzAsync(
      IPEndPoint serverEp,
      DatagramOptions baseOptions,
      SimulatedNetworkHub? hub,
      CancellationToken ct)
   {
      if (hub is not null && Random.Shared.Next(2) == 0)
      {
         var fakeSrc = new IPEndPoint(IPAddress.Parse("198.51.100.42"), Random.Shared.Next(10000, 60000));
         var garbage = new byte[Random.Shared.Next(5, 180)];
         Random.Shared.NextBytes(garbage);
         hub.InjectRawPacket(fakeSrc, serverEp, garbage);
         Interlocked.Increment(ref Program.ClientFuzzPacketsSent);
      }

      var rejectOpts = Program.CloneClientOptions(baseOptions);
      var badPsk = rejectOpts.Security.Mode == DatagramSecurityMode.Aead && Random.Shared.Next(2) == 0;
      if (badPsk)
      {
         rejectOpts.Security.PreSharedKey = "wrong-chaos-psk"u8.ToArray();
         rejectOpts.ConnectTimeout = TimeSpan.FromMilliseconds(500);
      }

      var transport = hub is not null ? (IDatagramTransport)hub.CreateTransport() : null;
      await using var host = new DatagramHost(
         new IPEndPoint(IPAddress.Loopback, 0),
         rejectOpts,
         transport,
         hub?.Time);
      host.Start();

      Interlocked.Increment(ref Program.ClientConnectAttempts);
      try
      {
         await host.ConnectAsync(serverEp, "banned-chaos-client"u8.ToArray(), ct);
         Interlocked.Increment(ref Program.ClientConnectFailUnexpected);
      }
      catch (DatagramConnectionRejectedException)
      {
         Interlocked.Increment(ref Program.ClientConnectFailExpected);
      }
      catch (TimeoutException) when (badPsk)
      {
         Interlocked.Increment(ref Program.ClientConnectFailExpected);
      }
      catch (OperationCanceledException)
      {
      }
   }

   private static void RecordSendResult(DatagramSendResult result, byte channelId)
   {
      if (result == DatagramSendResult.Queued)
      {
         Interlocked.Increment(ref Program.ClientMessagesSentTotal);
         switch (channelId)
         {
            case 0: Interlocked.Increment(ref Program.ClientSentCh0Ordered); break;
            case 1: Interlocked.Increment(ref Program.ClientSentCh1Unordered); break;
            case 2: Interlocked.Increment(ref Program.ClientSentCh2UnrelSeq); break;
            case 3: Interlocked.Increment(ref Program.ClientSentCh3UnrelUnord); break;
            case 4: Interlocked.Increment(ref Program.ClientSentCh4RelSeq); break;
         }
      }
      else if (result == DatagramSendResult.QueueFull)
      {
         Interlocked.Increment(ref Program.ClientSendQueueFull);
      }
   }
}
