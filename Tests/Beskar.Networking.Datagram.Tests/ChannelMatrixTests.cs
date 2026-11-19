using System.Buffers.Binary;
using System.Net;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Networking.Datagram.Tests;

public class ChannelMatrixTests
{
   [Test]
   [Arguments(DatagramSecurityMode.None)]
   [Arguments(DatagramSecurityMode.Crc32C)]
   [Arguments(DatagramSecurityMode.Aead)]
   public async Task SecurityModeMatrix_AllFiveDeliveryModes_WorkUnderLossAndReordering(DatagramSecurityMode securityMode)
   {
      var hub = new SimulatedNetworkHub(seed: 2000 + (int)securityMode, chaos: new SimulatedChaosProfile
      {
         PacketDropRate = 0.20,
         PacketDuplicateRate = 0.15,
         ReorderRate = 0.25,
         MinLatency = TimeSpan.FromMilliseconds(5),
         MaxLatency = TimeSpan.FromMilliseconds(30)
      });

      var serverEp = new IPEndPoint(IPAddress.Loopback, 9950 + (int)securityMode);
      DatagramOptions MakeOpts() => new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero,
         Security = new DatagramSecurityOptions { Mode = securityMode }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered, windowSize: 64)
      .AddChannel(1, DatagramDeliveryMode.ReliableUnordered, windowSize: 64)
      .AddChannel(2, DatagramDeliveryMode.UnreliableSequenced)
      .AddChannel(3, DatagramDeliveryMode.UnreliableUnordered)
      .AddChannel(4, DatagramDeliveryMode.ReliableSequenced, windowSize: 32);

      var server = new DatagramHost(serverEp, MakeOpts(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts(), hub.CreateTransport(), hub.Time);
      server.Start(); client.Start();

      var ct = client.ConnectAsync(serverEp);
      for (var i = 0; i < 60 && !ct.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var peer = await ct;
      server.PollEvents(_ => { });

      const int count = 60;
      var ordered = new List<int>();
      var unordered = new List<int>();
      var seq = new List<int>();
      var fireAndForget = 0;
      var relSeq = new List<int>();

      for (var i = 0; i < count; i++)
      {
         var b = BitConverter.GetBytes(i);
         peer.TrySend(0, b);
         peer.TrySend(1, b);
         peer.TrySend(2, b);
         peer.TrySend(3, b);
         peer.TrySend(4, b);

         if (i % 3 == 0)
         {
            Step();
         }
      }

      for (var t = 0; t < 250 && (ordered.Count < count || unordered.Count < count || relSeq.Count == 0 || relSeq[^1] != count - 1); t++)
      {
         Step();
      }

      await Assert.That(ordered.Count).IsEqualTo(count);
      for (var i = 0; i < count; i++) await Assert.That(ordered[i]).IsEqualTo(i);

      await Assert.That(unordered.Count).IsEqualTo(count);
      await Assert.That(unordered.Distinct().Count()).IsEqualTo(count);

      await Assert.That(seq.Count).IsGreaterThan(0);
      for (var i = 1; i < seq.Count; i++) await Assert.That(seq[i]).IsGreaterThan(seq[i - 1]);

      await Assert.That(fireAndForget).IsGreaterThan(0);
      await Assert.That(relSeq[^1]).IsEqualTo(count - 1);

      await client.DisposeAsync();
      await server.DisposeAsync();

      void Step()
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.PollEvents(evt =>
         {
            if (evt.Type != DatagramEventType.MessageReceived) return;
            var v = BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span);
            switch (evt.ChannelId)
            {
               case 0: ordered.Add(v); break;
               case 1: unordered.Add(v); break;
               case 2: seq.Add(v); break;
               case 3: fireAndForget++; break;
               case 4: relSeq.Add(v); break;
            }
         });
         client.PollEvents(_ => { });
      }
   }

   [Test]
   [Arguments(DatagramDeliveryMode.ReliableOrdered, 1)]
   [Arguments(DatagramDeliveryMode.ReliableOrdered, 3)]
   [Arguments(DatagramDeliveryMode.ReliableOrdered, 7)]
   [Arguments(DatagramDeliveryMode.ReliableOrdered, 31)]
   [Arguments(DatagramDeliveryMode.ReliableUnordered, 1)]
   [Arguments(DatagramDeliveryMode.ReliableUnordered, 7)]
   [Arguments(DatagramDeliveryMode.ReliableUnordered, 63)]
   [Arguments(DatagramDeliveryMode.ReliableSequenced, 1)]
   [Arguments(DatagramDeliveryMode.ReliableSequenced, 7)]
   public async Task TinyAndOddWindowSizes_IncludingStopAndWaitWindow1_DeliverReliablyUnderChaos(
      DatagramDeliveryMode mode,
      int windowSize)
   {
      var hub = new SimulatedNetworkHub(seed: 3000 + windowSize * 10 + (int)mode, chaos: new SimulatedChaosProfile
      {
         PacketDropRate = 0.20,
         PacketDuplicateRate = 0.15,
         ReorderRate = 0.25,
         MinLatency = TimeSpan.FromMilliseconds(5),
         MaxLatency = TimeSpan.FromMilliseconds(25)
      });

      var serverEp = new IPEndPoint(IPAddress.Loopback, 9970);
      DatagramOptions MakeOpts() => new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero,
         PeerTimeout = TimeSpan.FromSeconds(30),
         MaxReliableRetryDuration = TimeSpan.FromSeconds(30),
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Crc32C }
      }.AddChannel(0, mode, windowSize: windowSize, maxSendQueueDepth: 256);

      var server = new DatagramHost(serverEp, MakeOpts(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts(), hub.CreateTransport(), hub.Time);
      server.Start(); client.Start();

      var ct = client.ConnectAsync(serverEp);
      for (var i = 0; i < 60 && !ct.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var peer = await ct;
      server.PollEvents(_ => { });

      const int count = 40;
      var received = new List<int>();
      for (var i = 0; i < count; i++)
      {
         var b = BitConverter.GetBytes(i);
         while (peer.TrySend(0, b) == DatagramSendResult.QueueFull)
         {
            Step();
         }

         if (mode == DatagramDeliveryMode.ReliableSequenced && i % 5 == 0)
         {
            Step();
         }
      }

      for (var t = 0; t < 600; t++)
      {
         Step();
         if (mode == DatagramDeliveryMode.ReliableSequenced && received.Count > 0 && received[^1] == count - 1)
            break;
         if (mode != DatagramDeliveryMode.ReliableSequenced && received.Count == count)
            break;
      }

      if (mode == DatagramDeliveryMode.ReliableOrdered)
      {
         await Assert.That(received.Count).IsEqualTo(count);
         for (var i = 0; i < count; i++) await Assert.That(received[i]).IsEqualTo(i);
      }
      else if (mode == DatagramDeliveryMode.ReliableUnordered)
      {
         await Assert.That(received.Count).IsEqualTo(count);
         await Assert.That(received.Distinct().Count()).IsEqualTo(count);
      }
      else
      {
         await Assert.That(received.Count).IsGreaterThan(0);
         await Assert.That(received[^1]).IsEqualTo(count - 1);
         for (var i = 1; i < received.Count; i++) await Assert.That(received[i]).IsGreaterThan(received[i - 1]);
      }

      await client.DisposeAsync();
      await server.DisposeAsync();

      void Step()
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.MessageReceived)
               received.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
         });
         client.PollEvents(_ => { });
      }
   }
}
