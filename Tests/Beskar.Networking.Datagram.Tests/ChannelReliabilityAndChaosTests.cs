using System.Buffers.Binary;
using System.Net;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Networking.Datagram.Tests;

public class ChannelReliabilityAndChaosTests
{
   private static DatagramOptions CreateTestOptions(DatagramSecurityMode securityMode = DatagramSecurityMode.Aead)
   {
      return new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero,
         Security = new DatagramSecurityOptions { Mode = securityMode }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered, priority: 255, windowSize: 300) // Non-power-of-2 window!
      .AddChannel(1, DatagramDeliveryMode.ReliableUnordered, priority: 200, windowSize: 300)
      .AddChannel(2, DatagramDeliveryMode.UnreliableSequenced, priority: 150)
      .AddChannel(3, DatagramDeliveryMode.UnreliableUnordered, priority: 100)
      .AddChannel(4, DatagramDeliveryMode.ReliableSequenced, priority: 180, windowSize: 128);
   }

   private static async Task<(DatagramHost Server, DatagramHost Client, DatagramPeer ClientPeer, DatagramPeer ServerPeer)>
      ConnectSimulatedPairAsync(SimulatedNetworkHub hub, DatagramOptions? serverOpts = null, DatagramOptions? clientOpts = null)
   {
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9000);
      var server = new DatagramHost(serverEp, serverOpts ?? CreateTestOptions(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), clientOpts ?? CreateTestOptions(), hub.CreateTransport(), hub.Time);

      server.Start();
      client.Start();

      var connectTask = client.ConnectAsync(serverEp);
      for (var i = 0; i < 60 && !connectTask.IsCompleted; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }

      var clientPeer = await connectTask;
      DatagramPeer? serverPeer = null;
      server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.PeerConnected)
         {
            serverPeer = evt.Peer;
         }
      });

      client.PollEvents(_ => { });
      await Assert.That(serverPeer).IsNotNull();
      return (server, client, clientPeer, serverPeer!);
   }

   [Test]
   public async Task AllFiveChannels_OperateReliablyAndCorrectlyUnderHeavyChaos()
   {
      var chaos = new SimulatedChaosProfile
      {
         PacketDropRate = 0.25,
         PacketDuplicateRate = 0.15,
         ReorderRate = 0.30,
         DataCorruptionRate = 0.04,
         MinLatency = TimeSpan.FromMilliseconds(10),
         MaxLatency = TimeSpan.FromMilliseconds(60),
         ReorderExtraDelay = TimeSpan.FromMilliseconds(45)
      };

      var hub = new SimulatedNetworkHub(seed: 42, chaos: chaos);
      var (server, client, clientPeer, _) = await ConnectSimulatedPairAsync(hub);

      const int reliableOrderedCount = 250;
      const int reliableUnorderedCount = 150;
      const int unreliableSeqCount = 80;
      const int unreliableUnorderedCount = 60;

      var recvOrdered = new List<int>();
      var recvUnordered = new List<int>();
      var recvSeq = new List<int>();
      var recvFireAndForget = 0;
      var recvRelSeq = new List<int>();

      var msg = new byte[4];

      for (var i = 0; i < reliableOrderedCount; i++)
      {
         BinaryPrimitives.WriteInt32LittleEndian(msg, i);
         await Assert.That(clientPeer.TrySend(0, msg)).IsEqualTo(DatagramSendResult.Queued);

         if (i < reliableUnorderedCount)
         {
            await Assert.That(clientPeer.TrySend(1, msg)).IsEqualTo(DatagramSendResult.Queued);
         }

         if (i < unreliableSeqCount)
         {
            await Assert.That(clientPeer.TrySend(2, msg)).IsEqualTo(DatagramSendResult.Queued);
         }

         if (i < unreliableUnorderedCount)
         {
            await Assert.That(clientPeer.TrySend(3, msg)).IsEqualTo(DatagramSendResult.Queued);
         }

         if (i < 40)
         {
            await Assert.That(clientPeer.TrySend(4, msg)).IsEqualTo(DatagramSendResult.Queued);
         }

         // Interleave ticks every few messages so queues stay well below window bounds and simulate live frames
         if (i % 4 == 0)
         {
            StepAndDrain(hub, client, server, recvOrdered, recvUnordered, recvSeq, ref recvFireAndForget, recvRelSeq);
         }
      }

      // Pump virtual clock so all retransmissions finish under 25% drop
      for (var tick = 0; tick < 300; tick++)
      {
         StepAndDrain(hub, client, server, recvOrdered, recvUnordered, recvSeq, ref recvFireAndForget, recvRelSeq);
         if (recvOrdered.Count == reliableOrderedCount
             && recvUnordered.Count == reliableUnorderedCount
             && recvRelSeq.Count > 0 && recvRelSeq[^1] == 39)
         {
            break;
         }
      }

      // 1. ReliableOrdered: 100% received in strict FIFO order!
      await Assert.That(recvOrdered.Count).IsEqualTo(reliableOrderedCount);
      for (var i = 0; i < reliableOrderedCount; i++)
      {
         await Assert.That(recvOrdered[i]).IsEqualTo(i);
      }

      // 2. ReliableUnordered: 100% received with zero duplicates!
      await Assert.That(recvUnordered.Count).IsEqualTo(reliableUnorderedCount);
      var distinctUnordered = recvUnordered.Distinct().Order().ToList();
      await Assert.That(distinctUnordered.Count).IsEqualTo(reliableUnorderedCount);

      // 3. UnreliableSequenced: strictly increasing, drops allowed!
      await Assert.That(recvSeq.Count).IsGreaterThan(0);
      await Assert.That(recvSeq.Count).IsLessThanOrEqualTo(unreliableSeqCount);
      for (var i = 1; i < recvSeq.Count; i++)
      {
         await Assert.That(recvSeq[i]).IsGreaterThan(recvSeq[i - 1]);
      }

      // 4. UnreliableUnordered: drops allowed, some arrived!
      await Assert.That(recvFireAndForget).IsGreaterThan(0);

      // 5. ReliableSequenced: strictly increasing and guaranteed to converge on final value (39)!
      await Assert.That(recvRelSeq[^1]).IsEqualTo(39);
      for (var i = 1; i < recvRelSeq.Count; i++)
      {
         await Assert.That(recvRelSeq[i]).IsGreaterThan(recvRelSeq[i - 1]);
      }

      await client.DisposeAsync();
      await server.DisposeAsync();

      static void StepAndDrain(
         SimulatedNetworkHub hub,
         DatagramHost client,
         DatagramHost server,
         List<int> ordered,
         List<int> unordered,
         List<int> seq,
         ref int fireAndForget,
         List<int> relSeq)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));

         var ff = fireAndForget;
         server.PollEvents(evt =>
         {
            if (evt.Type != DatagramEventType.MessageReceived) return;
            var val = BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span);
            switch (evt.ChannelId)
            {
               case 0: ordered.Add(val); break;
               case 1: unordered.Add(val); break;
               case 2: seq.Add(val); break;
               case 3: ff++; break;
               case 4: relSeq.Add(val); break;
            }
         });
         fireAndForget = ff;
         client.PollEvents(_ => { });
      }
   }

   [Test]
   public async Task LargeFragmentedMessages_ReassembleCorrectlyUnderLossAndReordering()
   {
      var chaos = new SimulatedChaosProfile
      {
         PacketDropRate = 0.20,
         ReorderRate = 0.35,
         MinLatency = TimeSpan.FromMilliseconds(5),
         MaxLatency = TimeSpan.FromMilliseconds(35),
         ReorderExtraDelay = TimeSpan.FromMilliseconds(40)
      };

      var hub = new SimulatedNetworkHub(seed: 99, chaos: chaos);
      var (server, client, clientPeer, _) = await ConnectSimulatedPairAsync(hub);

      var largePayload1 = new byte[14_500];
      var largePayload2 = new byte[9_300];
      new Random(7).NextBytes(largePayload1);
      new Random(8).NextBytes(largePayload2);

      // Send 1 fragmented ReliableOrdered + 1 fragmented ReliableUnordered + 1 fragmented ReliableSequenced
      await Assert.That(clientPeer.TrySend(0, largePayload1)).IsEqualTo(DatagramSendResult.Queued);
      await Assert.That(clientPeer.TrySend(1, largePayload2)).IsEqualTo(DatagramSendResult.Queued);
      await Assert.That(clientPeer.TrySend(4, largePayload1)).IsEqualTo(DatagramSendResult.Queued);
      await Assert.That(clientPeer.TrySend(4, largePayload2)).IsEqualTo(DatagramSendResult.Queued); // supersedes largePayload1 on ch4!

      byte[]? gotCh0 = null;
      byte[]? gotCh1 = null;
      var ch4Deliveries = new List<byte[]>();

      for (var tick = 0; tick < 250; tick++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));

         server.PollEvents(evt =>
         {
            if (evt.Type != DatagramEventType.MessageReceived) return;
            switch (evt.ChannelId)
            {
               case 0: gotCh0 = evt.Payload.ToArray(); break;
               case 1: gotCh1 = evt.Payload.ToArray(); break;
               case 4: ch4Deliveries.Add(evt.Payload.ToArray()); break;
            }
         });
         client.PollEvents(_ => { });

         if (gotCh0 is not null && gotCh1 is not null && ch4Deliveries.Count > 0)
         {
            break;
         }
      }

      await Assert.That(gotCh0).IsEquivalentTo(largePayload1);
      await Assert.That(gotCh1).IsEquivalentTo(largePayload2);
      await Assert.That(ch4Deliveries[^1]).IsEquivalentTo(largePayload2);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task SequenceWrapAround_Across65535_WorksWithNonPowerOfTwoWindow()
   {
      var hub = new SimulatedNetworkHub(seed: 123, chaos: SimulatedChaosProfile.Clean);
      var (server, client, clientPeer, _) = await ConnectSimulatedPairAsync(
         hub,
         CreateTestOptions(DatagramSecurityMode.Crc32C),
         CreateTestOptions(DatagramSecurityMode.Crc32C));

      var receivedCount = 0;
      var expected = 0;
      var buf = new byte[4];

      // Send 66,000 messages across ushort wrap boundary (0 -> 65535 -> 0 -> 464) on non-power-of-2 window (300)
      const int total = 66_000;
      for (var i = 0; i < total; i++)
      {
         BinaryPrimitives.WriteInt32LittleEndian(buf, i);
         while (clientPeer.TrySend(0, buf) == DatagramSendResult.QueueFull)
         {
            client.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            server.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
            Drain();
         }

         if (i % 64 == 0)
         {
            client.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(5));
            server.Tick();
            hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(5));
            Drain();
         }
      }

      for (var tick = 0; tick < 200 && receivedCount < total; tick++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(10));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(10));
         Drain();
      }

      await Assert.That(receivedCount).IsEqualTo(total);

      await client.DisposeAsync();
      await server.DisposeAsync();

      void Drain()
      {
         server.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.MessageReceived)
            {
               var v = BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span);
               if (v != expected)
               {
                  throw new InvalidOperationException($"Out of order at {expected}: got {v}");
               }
               expected++;
               receivedCount++;
            }
         });
         client.PollEvents(_ => { });
      }
   }

   [Test]
   public async Task StalledReliableOrderedChannel_DoesNotBlockOtherChannels()
   {
      var hub = new SimulatedNetworkHub(seed: 10);
      var (server, client, clientPeer, _) = await ConnectSimulatedPairAsync(hub);

      // Drop the very next packet (carrying Ch0 seq=0)
      hub.Chaos.PacketDropRate = 1.0;
      clientPeer.TrySend(0, BitConverter.GetBytes(0));
      client.Tick();
      hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));

      // Re-enable network; send Ch0 seq=1..3 plus Ch1 and Ch2 messages
      hub.Chaos.PacketDropRate = 0.0;
      clientPeer.TrySend(0, BitConverter.GetBytes(1));
      clientPeer.TrySend(0, BitConverter.GetBytes(2));
      clientPeer.TrySend(1, BitConverter.GetBytes(101));
      clientPeer.TrySend(2, BitConverter.GetBytes(202));

      client.Tick();
      hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(10));
      server.Tick();

      var ch0 = new List<int>();
      var ch1 = new List<int>();
      var ch2 = new List<int>();

      server.PollEvents(evt =>
      {
         if (evt.Type != DatagramEventType.MessageReceived) return;
         var v = BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span);
         switch (evt.ChannelId)
         {
            case 0: ch0.Add(v); break;
            case 1: ch1.Add(v); break;
            case 2: ch2.Add(v); break;
         }
      });

      // Ch0 is waiting for seq=0 (held), while Ch1 and Ch2 delivered immediately with zero HoL blocking!
      await Assert.That(ch0.Count).IsEqualTo(0);
      await Assert.That(ch1).IsEquivalentTo([101]);
      await Assert.That(ch2).IsEquivalentTo([202]);

      // Advance time past RTO so seq=0 retransmits -> Ch0 drains 0, 1, 2 in strict order!
      for (var i = 0; i < 30 && ch0.Count < 3; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(25));
         server.Tick();
         server.PollEvents(evt =>
         {
            if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 0)
            {
               ch0.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
            }
         });
         client.PollEvents(_ => { });
      }

      await Assert.That(ch0).IsEquivalentTo([0, 1, 2]);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}
