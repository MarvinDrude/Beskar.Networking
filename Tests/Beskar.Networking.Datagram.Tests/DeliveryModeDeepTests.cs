using System.Buffers.Binary;
using System.Net;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Protocol;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Networking.Datagram.Tests;

public class DeliveryModeDeepTests
{
   private static DatagramOptions CreateAllModesOptions(
      DatagramSecurityMode security = DatagramSecurityMode.Aead,
      int reliableWindow = 128,
      int maxQueue = 512,
      bool allowUnreliableFrag = false)
   {
      return new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero,
         PeerTimeout = TimeSpan.FromSeconds(15),
         MaxReliableRetryDuration = TimeSpan.FromSeconds(15),
         Security = new DatagramSecurityOptions { Mode = security }
      }
      .AddChannel(0, DatagramDeliveryMode.ReliableOrdered, priority: 255, windowSize: reliableWindow, maxSendQueueDepth: maxQueue, allowFragmentation: true)
      .AddChannel(1, DatagramDeliveryMode.ReliableUnordered, priority: 220, windowSize: reliableWindow, maxSendQueueDepth: maxQueue, allowFragmentation: true)
      .AddChannel(2, DatagramDeliveryMode.UnreliableSequenced, priority: 150, allowFragmentation: allowUnreliableFrag)
      .AddChannel(3, DatagramDeliveryMode.UnreliableUnordered, priority: 100, allowFragmentation: allowUnreliableFrag)
      .AddChannel(4, DatagramDeliveryMode.ReliableSequenced, priority: 180, windowSize: reliableWindow, maxSendQueueDepth: maxQueue, allowFragmentation: true);
   }

   private static async Task<(DatagramHost Server, DatagramHost Client, DatagramPeer ClientPeer, DatagramPeer ServerPeer)>
      ConnectPairAsync(SimulatedNetworkHub hub, DatagramOptions? serverOpts = null, DatagramOptions? clientOpts = null)
   {
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9600);
      var server = new DatagramHost(serverEp, serverOpts ?? CreateAllModesOptions(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), clientOpts ?? CreateAllModesOptions(), hub.CreateTransport(), hub.Time);
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
         if (evt.Type == DatagramEventType.PeerConnected) serverPeer = evt.Peer;
      });
      client.PollEvents(_ => { });

      await Assert.That(serverPeer).IsNotNull();
      return (server, client, clientPeer, serverPeer!);
   }

   private static void Pump(
      SimulatedNetworkHub hub,
      DatagramHost client,
      DatagramHost server,
      int ticks,
      int stepMs,
      Action<DatagramEvent>? onServerEvent = null,
      Action<DatagramEvent>? onClientEvent = null)
   {
      for (var i = 0; i < ticks; i++)
      {
         client.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(stepMs));
         server.Tick();
         hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(stepMs));

         server.PollEvents(evt => onServerEvent?.Invoke(evt));
         client.PollEvents(evt => onClientEvent?.Invoke(evt));
      }
   }

   // =========================================================================
   // 1. UNRELIABLE UNORDERED
   // =========================================================================

   [Test]
   public async Task UnreliableUnordered_DropsPacketWithoutRetransmit_AndAcceptsReverseOrderAndEmptyPayload()
   {
      var hub = new SimulatedNetworkHub(seed: 101);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub, CreateAllModesOptions(DatagramSecurityMode.Crc32C), CreateAllModesOptions(DatagramSecurityMode.Crc32C));

      var captured = new List<(IPEndPoint Src, IPEndPoint Dst, byte[] Data)>();
      var sendCount = 0;

      // Capture 4 separate Data packets: drop #2, reverse #3 and #1, keep #4 (0-byte payload)
      hub.OnPacketSending = (src, dst, raw) =>
      {
         if (!src.Equals(client.LocalEndPoint)) return true;
         if (!DatagramWireHeader.TryRead(raw, out var hdr) || hdr.Kind != DatagramPacketKind.Data) return true;

         sendCount++;
         if (sendCount == 2) return false; // drop packet #2 permanently!

         captured.Add((src, dst, (byte[])raw.Clone()));
         return false; // hold so we can inject in exact reverse order
      };

      for (var i = 1; i <= 3; i++)
      {
         clientPeer.TrySend(3, BitConverter.GetBytes(i));
         client.Tick();
      }

      // Send a 0-byte empty payload as packet #4
      clientPeer.TrySend(3, ReadOnlySpan<byte>.Empty);
      client.Tick();

      hub.OnPacketSending = null;
      await Assert.That(captured.Count).IsEqualTo(3); // #1, #3, #4

      // Inject in reverse order: #4 (empty), #3, #1
      for (var i = captured.Count - 1; i >= 0; i--)
      {
         hub.InjectRawPacket(captured[i].Src, captured[i].Dst, captured[i].Data);
      }

      // Also inject a duplicate of #3 to prove AckTracker anti-replay prevents duplicate app delivery
      hub.InjectRawPacket(captured[1].Src, captured[1].Dst, captured[1].Data);

      var receivedLengthsAndValues = new List<(int Len, int Val)>();
      Pump(hub, client, server, ticks: 25, stepMs: 40, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 3)
         {
            var val = evt.Payload.Length == 4 ? BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span) : -1;
            receivedLengthsAndValues.Add((evt.Payload.Length, val));
         }
      });

      // Exact reverse order: empty (-1), 3, 1. Packet #2 was NEVER retransmitted, duplicate #3 dropped!
      await Assert.That(receivedLengthsAndValues.Count).IsEqualTo(3);
      await Assert.That(receivedLengthsAndValues[0]).IsEqualTo((0, -1));
      await Assert.That(receivedLengthsAndValues[1]).IsEqualTo((4, 3));
      await Assert.That(receivedLengthsAndValues[2]).IsEqualTo((4, 1));
      await Assert.That(clientPeer.Stats.MessagesRetransmitted).IsEqualTo(0);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task UnreliableUnordered_WithFragmentationEnabled_DropsIncompleteAndDeliversComplete()
   {
      var hub = new SimulatedNetworkHub(seed: 102);
      var opts = CreateAllModesOptions(DatagramSecurityMode.Crc32C, allowUnreliableFrag: true);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub, opts, CreateAllModesOptions(DatagramSecurityMode.Crc32C, allowUnreliableFrag: true));

      var payloadA = new byte[3500];
      var payloadB = new byte[3500];
      new Random(1).NextBytes(payloadA);
      new Random(2).NextBytes(payloadB);

      // Drop only the 2nd datagram of payloadA -> payloadA must NOT deliver
      var pktNum = 0;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (!src.Equals(client.LocalEndPoint)) return true;
         if (!DatagramWireHeader.TryRead(raw, out var hdr) || hdr.Kind != DatagramPacketKind.Data) return true;
         pktNum++;
         return pktNum != 2;
      };

      await Assert.That(clientPeer.TrySend(3, payloadA)).IsEqualTo(DatagramSendResult.Queued);
      var delivered = new List<byte[]>();
      Pump(hub, client, server, ticks: 10, stepMs: 25, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 3)
            delivered.Add(evt.Payload.ToArray());
      });

      await Assert.That(delivered.Count).IsEqualTo(0);

      // Now send payloadB cleanly -> must reassemble and deliver intact
      hub.OnPacketSending = null;
      await Assert.That(clientPeer.TrySend(3, payloadB)).IsEqualTo(DatagramSendResult.Queued);
      Pump(hub, client, server, ticks: 10, stepMs: 25, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 3)
            delivered.Add(evt.Payload.ToArray());
      });

      await Assert.That(delivered.Count).IsEqualTo(1);
      await Assert.That(delivered[0]).IsEquivalentTo(payloadB);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   // =========================================================================
   // 2. UNRELIABLE SEQUENCED
   // =========================================================================

   [Test]
   public async Task UnreliableSequenced_DropsStaleOutOfOrderPacketsAndNeverRetransmits()
   {
      var hub = new SimulatedNetworkHub(seed: 201);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub);

      var captured = new List<(IPEndPoint Src, IPEndPoint Dst, byte[] Data)>();
      hub.OnPacketSending = (src, dst, raw) =>
      {
         if (!src.Equals(client.LocalEndPoint)) return true;
         if (!DatagramWireHeader.TryRead(raw, out var hdr) || hdr.Kind != DatagramPacketKind.Data) return true;
         captured.Add((src, dst, (byte[])raw.Clone()));
         return false;
      };

      for (var i = 1; i <= 5; i++)
      {
         clientPeer.TrySend(2, BitConverter.GetBytes(i));
         client.Tick();
      }
      hub.OnPacketSending = null;

      await Assert.That(captured.Count).IsEqualTo(5);

      // Deliver in order: #1, #4, #2 (stale -> drop), #3 (stale -> drop), #5 (dropped permanently!)
      hub.InjectRawPacket(captured[0].Src, captured[0].Dst, captured[0].Data); // 1
      hub.InjectRawPacket(captured[3].Src, captured[3].Dst, captured[3].Data); // 4
      hub.InjectRawPacket(captured[1].Src, captured[1].Dst, captured[1].Data); // 2 (stale)
      hub.InjectRawPacket(captured[2].Src, captured[2].Dst, captured[2].Data); // 3 (stale)

      var received = new List<int>();
      Pump(hub, client, server, ticks: 25, stepMs: 40, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 2)
            received.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
      });

      await Assert.That(received).IsEquivalentTo([1, 4]);
      await Assert.That(clientPeer.Stats.MessagesRetransmitted).IsEqualTo(0);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task UnreliableSequenced_FragmentedOutOfOrderFragmentsReassemble_AndNewerClearsOlderIncomplete()
   {
      var hub = new SimulatedNetworkHub(seed: 202);
      var (server, client, clientPeer, _) = await ConnectPairAsync(
         hub,
         CreateAllModesOptions(DatagramSecurityMode.Crc32C, allowUnreliableFrag: true),
         CreateAllModesOptions(DatagramSecurityMode.Crc32C, allowUnreliableFrag: true));

      var fragPayload = new byte[3400];
      new Random(55).NextBytes(fragPayload);

      var captured = new List<(IPEndPoint Src, IPEndPoint Dst, byte[] Data)>();
      hub.OnPacketSending = (src, dst, raw) =>
      {
         if (!src.Equals(client.LocalEndPoint)) return true;
         if (!DatagramWireHeader.TryRead(raw, out var hdr) || hdr.Kind != DatagramPacketKind.Data) return true;
         captured.Add((src, dst, (byte[])raw.Clone()));
         return false;
      };

      await Assert.That(clientPeer.TrySend(2, fragPayload)).IsEqualTo(DatagramSendResult.Queued);
      client.Tick();
      hub.OnPacketSending = null;

      await Assert.That(captured.Count).IsGreaterThanOrEqualTo(3);

      // Deliver fragments in REVERSE order!
      for (var i = captured.Count - 1; i >= 0; i--)
      {
         hub.InjectRawPacket(captured[i].Src, captured[i].Dst, captured[i].Data);
      }

      var received = new List<byte[]>();
      Pump(hub, client, server, ticks: 5, stepMs: 10, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 2)
            received.Add(evt.Payload.ToArray());
      });

      await Assert.That(received.Count).IsEqualTo(1);
      await Assert.That(received[0]).IsEquivalentTo(fragPayload);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   // =========================================================================
   // 3. RELIABLE UNORDERED
   // =========================================================================

   [Test]
   public async Task ReliableUnordered_DeliversNewerMessagesImmediatelyWithoutWaitingOnDroppedGap()
   {
      var hub = new SimulatedNetworkHub(seed: 301);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub);

      // Drop ONLY the first Data packet (#0); let #1 and #2 pass immediately
      var dataPktIdx = 0;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (!src.Equals(client.LocalEndPoint)) return true;
         if (!DatagramWireHeader.TryRead(raw, out var hdr) || hdr.Kind != DatagramPacketKind.Data) return true;
         dataPktIdx++;
         return dataPktIdx != 1;
      };

      clientPeer.TrySend(1, BitConverter.GetBytes(0));
      client.Tick();
      clientPeer.TrySend(1, BitConverter.GetBytes(1));
      client.Tick();
      clientPeer.TrySend(1, BitConverter.GetBytes(2));
      client.Tick();

      hub.OnPacketSending = null;

      // Deliver #1 and #2 immediately (no wait for RTO!)
      var arrivalOrder = new List<int>();
      server.Tick();
      server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 1)
            arrivalOrder.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
      });

      // #1 and #2 MUST already be delivered BEFORE #0 retransmits (zero HoL blocking!)
      await Assert.That(arrivalOrder).IsEquivalentTo([1, 2]);

      // Now advance past RTO so #0 retransmits and arrives
      Pump(hub, client, server, ticks: 30, stepMs: 25, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 1)
            arrivalOrder.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
      });

      await Assert.That(arrivalOrder).IsEquivalentTo([1, 2, 0]);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ReliableUnordered_AsymmetricAckLossAndHeavyDuplication_ZeroDuplicates100PercentDelivery()
   {
      var hub = new SimulatedNetworkHub(seed: 302);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub);

      // Server -> Client drops 70% of ACKs (forcing heavy retransmissions of already-delivered messages!)
      var rng = new Random(77);
      hub.OnPacketSending = (src, _, _) =>
      {
         if (src.Equals(server.LocalEndPoint) && rng.NextDouble() < 0.70) return false;
         return true;
      };

      const int count = 120;
      var received = new List<int>();
      for (var i = 0; i < count; i++)
      {
         clientPeer.TrySend(1, BitConverter.GetBytes(i));
         if (i % 5 == 0)
         {
            Pump(hub, client, server, ticks: 2, stepMs: 20, onServerEvent: evt =>
            {
               if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 1)
                  received.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
            });
         }
      }

      Pump(hub, client, server, ticks: 250, stepMs: 25, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 1)
            received.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
      });

      await Assert.That(clientPeer.Stats.MessagesRetransmitted).IsGreaterThan(0);
      await Assert.That(received.Count).IsEqualTo(count); // Exact count -> ZERO duplicates!
      await Assert.That(received.Distinct().Count()).IsEqualTo(count);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   // =========================================================================
   // 4. RELIABLE ORDERED
   // =========================================================================

   [Test]
   public async Task ReliableOrdered_ExactReverseArrivalAndMultiGapDrops_DrainsStrictlyInFifoOrder()
   {
      var hub = new SimulatedNetworkHub(seed: 401);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub);

      var captured = new List<(IPEndPoint Src, IPEndPoint Dst, byte[] Data)>();
      hub.OnPacketSending = (src, dst, raw) =>
      {
         if (!src.Equals(client.LocalEndPoint)) return true;
         if (!DatagramWireHeader.TryRead(raw, out var hdr) || hdr.Kind != DatagramPacketKind.Data) return true;
         captured.Add((src, dst, (byte[])raw.Clone()));
         return false;
      };

      for (var i = 0; i < 6; i++)
      {
         clientPeer.TrySend(0, BitConverter.GetBytes(i));
         client.Tick();
      }
      hub.OnPacketSending = null;
      await Assert.That(captured.Count).IsEqualTo(6);

      var delivered = new List<int>();

      // Inject 5, 4, 3, 2, 1 (reverse order, missing 0!) -> ZERO messages delivered yet
      for (var i = 5; i >= 1; i--)
      {
         hub.InjectRawPacket(captured[i].Src, captured[i].Dst, captured[i].Data);
      }

      server.Tick();
      server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 0)
            delivered.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
      });
      await Assert.That(delivered.Count).IsEqualTo(0);

      // Now inject #0 -> ALL 6 messages (0..5) drain immediately in exact FIFO order!
      hub.InjectRawPacket(captured[0].Src, captured[0].Dst, captured[0].Data);
      server.Tick();
      server.PollEvents(evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 0)
            delivered.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
      });

      await Assert.That(delivered).IsEquivalentTo([0, 1, 2, 3, 4, 5]);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ReliableOrdered_FastRetransmit_TriggersBeforeRtoWhenThreeNewerPacketsAcked()
   {
      var hub = new SimulatedNetworkHub(seed: 402);
      var opts = CreateAllModesOptions();
      opts.MinRtoMs = 1000.0; // Huge RTO (1 full second!) so timeout CANNOT fire yet
      opts.MaxRtoMs = 2000.0;

      var (server, client, clientPeer, _) = await ConnectPairAsync(hub, opts, CreateAllModesOptions());
      // Configure client with 1000ms MinRto too
      await client.DisposeAsync();
      await server.DisposeAsync();

      var clientOpts = CreateAllModesOptions();
      clientOpts.MinRtoMs = 1000.0;
      clientOpts.MaxRtoMs = 2000.0;
      (server, client, clientPeer, _) = await ConnectPairAsync(hub, opts, clientOpts);

      // Drop ONLY Data packet #1; allow #2, #3, #4, #5 through!
      var dataIdx = 0;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (!src.Equals(client.LocalEndPoint)) return true;
         if (!DatagramWireHeader.TryRead(raw, out var hdr) || hdr.Kind != DatagramPacketKind.Data) return true;
         dataIdx++;
         return dataIdx != 1;
      };

      for (var i = 0; i < 5; i++)
      {
         clientPeer.TrySend(0, BitConverter.GetBytes(i));
         client.Tick();
      }
      hub.OnPacketSending = null;

      // Advance only 60ms total (way below 1000ms RTO!) -> server ACKs packets 2..5 -> triggers Fast Retransmit!
      var received = new List<int>();
      Pump(hub, client, server, ticks: 6, stepMs: 10, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 0)
            received.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
      });

      // All 5 arrived in order within ~60ms via Fast Retransmit (without waiting for 1000ms RTO!)
      await Assert.That(received).IsEquivalentTo([0, 1, 2, 3, 4]);
      await Assert.That(clientPeer.Stats.MessagesRetransmitted).IsGreaterThanOrEqualTo(1);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ReliableOrdered_BurstBlackoutAndFullDuplexExtremeChaos_Recovers100Percent()
   {
      var hub = new SimulatedNetworkHub(seed: 403, chaos: new SimulatedChaosProfile
      {
         PacketDropRate = 0.35,
         PacketDuplicateRate = 0.20,
         ReorderRate = 0.35,
         DataCorruptionRate = 0.05,
         MinLatency = TimeSpan.FromMilliseconds(5),
         MaxLatency = TimeSpan.FromMilliseconds(45)
      });

      var (server, client, clientPeer, serverPeer) = await ConnectPairAsync(hub);

      const int count = 100;
      var serverReceived = new List<int>();
      var clientReceived = new List<int>();

      for (var i = 0; i < count; i++)
      {
         clientPeer.TrySend(0, BitConverter.GetBytes(i));
         serverPeer.TrySend(0, BitConverter.GetBytes(i + 1000));

         // Simulate a total 1.2-second network blackout mid-stream!
         if (i == 50)
         {
            var prevDrop = hub.Chaos.PacketDropRate;
            hub.Chaos.PacketDropRate = 1.0;
            Pump(hub, client, server, ticks: 30, stepMs: 40);
            hub.Chaos.PacketDropRate = prevDrop;
         }

         if (i % 4 == 0)
         {
            Pump(hub, client, server, ticks: 2, stepMs: 15,
               onServerEvent: evt => { if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 0) serverReceived.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span)); },
               onClientEvent: evt => { if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 0) clientReceived.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span)); });
         }
      }

      for (var tick = 0; tick < 500 && (serverReceived.Count < count || clientReceived.Count < count); tick++)
      {
         Pump(hub, client, server, ticks: 1, stepMs: 25,
            onServerEvent: evt => { if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 0) serverReceived.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span)); },
            onClientEvent: evt => { if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 0) clientReceived.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span)); });
      }

      await Assert.That(serverReceived.Count).IsEqualTo(count);
      await Assert.That(clientReceived.Count).IsEqualTo(count);

      for (var i = 0; i < count; i++)
      {
         await Assert.That(serverReceived[i]).IsEqualTo(i);
         await Assert.That(clientReceived[i]).IsEqualTo(i + 1000);
      }

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ReliableOrdered_WindowSaturationAndBackpressure_ReturnsQueueFullAndRecovers()
   {
      var hub = new SimulatedNetworkHub(seed: 404);
      var opts = CreateAllModesOptions(reliableWindow: 16, maxQueue: 32);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub, opts, CreateAllModesOptions(reliableWindow: 16, maxQueue: 32));

      // Pause network so no ACKs return
      hub.Chaos.PacketDropRate = 1.0;

      for (var i = 0; i < 32; i++)
      {
         await Assert.That(clientPeer.TrySend(0, BitConverter.GetBytes(i))).IsEqualTo(DatagramSendResult.Queued);
      }

      // 33rd message MUST fail with QueueFull
      await Assert.That(clientPeer.TrySend(0, BitConverter.GetBytes(32))).IsEqualTo(DatagramSendResult.QueueFull);

      // Tick once -> at most WindowSize (16) move in-flight; still QueueFull because total = 32
      client.Tick();
      await Assert.That(clientPeer.TrySend(0, BitConverter.GetBytes(32))).IsEqualTo(DatagramSendResult.QueueFull);

      // Restore network -> ACKs drain window -> queue frees up and accepts more messages!
      hub.Chaos.PacketDropRate = 0.0;
      var received = new List<int>();
      Pump(hub, client, server, ticks: 40, stepMs: 25, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 0)
            received.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
      });

      await Assert.That(received.Count).IsEqualTo(32);
      await Assert.That(clientPeer.TrySend(0, BitConverter.GetBytes(32))).IsEqualTo(DatagramSendResult.Queued);

      Pump(hub, client, server, ticks: 10, stepMs: 20, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 0)
            received.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
      });

      await Assert.That(received.Count).IsEqualTo(33);
      await Assert.That(received[^1]).IsEqualTo(32);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   // =========================================================================
   // 5. RELIABLE SEQUENCED
   // =========================================================================

   [Test]
   public async Task ReliableSequenced_RapidBurstExceedingWindowBeforeTick_DoesNotDeadlockAndConvergesOnLatest()
   {
      var hub = new SimulatedNetworkHub(seed: 501);
      var opts = CreateAllModesOptions(reliableWindow: 32, maxQueue: 64);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub, opts, CreateAllModesOptions(reliableWindow: 32, maxQueue: 64));

      // Spam 500 updates BEFORE a single Tick() (exceeds WindowSize=32 by >15x!)
      for (var i = 0; i < 500; i++)
      {
         await Assert.That(clientPeer.TrySend(4, BitConverter.GetBytes(i))).IsEqualTo(DatagramSendResult.Queued);
      }

      // Drop the first 3 attempts of #499 -> must STILL keep retransmitting #499 until it lands!
      var dataAttempts = 0;
      hub.OnPacketSending = (src, _, raw) =>
      {
         if (!src.Equals(client.LocalEndPoint)) return true;
         if (!DatagramWireHeader.TryRead(raw, out var hdr) || hdr.Kind != DatagramPacketKind.Data) return true;
         dataAttempts++;
         return dataAttempts > 3;
      };

      var received = new List<int>();
      Pump(hub, client, server, ticks: 120, stepMs: 25, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 4)
            received.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
      });

      await Assert.That(received).IsEquivalentTo([499]);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ReliableSequenced_FragmentedRetransmitDuplicates_NeverDeliverCompletedGroupTwice()
   {
      var hub = new SimulatedNetworkHub(seed: 502);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub);

      var bigState = new byte[4000];
      new Random(999).NextBytes(bigState);

      // Drop ACKs from Server -> Client on first round so Client retransmits ALL fragments of bigState in NEW PacketSeqs!
      var serverAckDrops = 0;
      hub.OnPacketSending = (src, _, _) =>
      {
         if (src.Equals(server.LocalEndPoint) && serverAckDrops < 10)
         {
            serverAckDrops++;
            return false;
         }
         return true;
      };

      await Assert.That(clientPeer.TrySend(4, bigState)).IsEqualTo(DatagramSendResult.Queued);

      var deliveries = new List<byte[]>();
      Pump(hub, client, server, ticks: 80, stepMs: 25, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 4)
            deliveries.Add(evt.Payload.ToArray());
      });

      // Even though fragments were retransmitted in fresh PacketSeqs, bigState MUST be delivered EXACTLY ONCE!
      await Assert.That(clientPeer.Stats.MessagesRetransmitted).IsGreaterThan(0);
      await Assert.That(deliveries.Count).IsEqualTo(1);
      await Assert.That(deliveries[0]).IsEquivalentTo(bigState);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ChannelId63_EmptyReliableOrderedPayload_AndReliableUnorderedWrapAround_AllWorkCleanly()
   {
      var hub = new SimulatedNetworkHub(seed: 503);
      var serverEp = new IPEndPoint(IPAddress.Loopback, 9650);

      DatagramOptions MakeOpts() => new DatagramOptions
      {
         ManualTick = true,
         DispatchMode = DatagramEventDispatchMode.Poll,
         MaxBytesPerSecondPerPeer = null,
         TimeSyncInterval = TimeSpan.Zero,
         Security = new DatagramSecurityOptions { Mode = DatagramSecurityMode.Crc32C }
      }
      .AddChannel(63, DatagramDeliveryMode.ReliableOrdered, priority: 255) // max 6-bit channel ID!
      .AddChannel(62, DatagramDeliveryMode.ReliableUnordered, windowSize: 200, maxSendQueueDepth: 1024);

      var server = new DatagramHost(serverEp, MakeOpts(), hub.CreateTransport(), hub.Time);
      var client = new DatagramHost(new IPEndPoint(IPAddress.Loopback, 0), MakeOpts(), hub.CreateTransport(), hub.Time);
      server.Start(); client.Start();

      var t = client.ConnectAsync(serverEp);
      for (var i = 0; i < 20 && !t.IsCompleted; i++)
      {
         client.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
         server.Tick(); hub.AdvanceAndDeliver(TimeSpan.FromMilliseconds(20));
      }
      var peer = await t;
      server.PollEvents(_ => { });

      // Send empty 0-byte message on ChannelId=63 (verifies bit 6 IsFragment doesn't bleed into channelId 0x3F!)
      // Plus a fragmented message on ChannelId=63 (0x3F | 0x40 = 0x7F!)
      var bigCh63 = new byte[2600];
      new Random(63).NextBytes(bigCh63);
      await Assert.That(peer.TrySend(63, ReadOnlySpan<byte>.Empty)).IsEqualTo(DatagramSendResult.Queued);
      await Assert.That(peer.TrySend(63, bigCh63)).IsEqualTo(DatagramSendResult.Queued);

      var ch63Deliveries = new List<byte[]>();
      Pump(hub, client, server, ticks: 20, stepMs: 20, onServerEvent: evt =>
      {
         if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 63)
            ch63Deliveries.Add(evt.Payload.ToArray());
      });

      await Assert.That(ch63Deliveries.Count).IsEqualTo(2);
      await Assert.That(ch63Deliveries[0].Length).IsEqualTo(0);
      await Assert.That(ch63Deliveries[1]).IsEquivalentTo(bigCh63);

      // Wrap ReliableUnordered (ch 62) past 65,535 with non-power-of-2 WindowSize = 200!
      const int wrapTotal = 66_000;
      var ch62Count = 0;
      var buf = new byte[4];
      for (var i = 0; i < wrapTotal; i++)
      {
         BinaryPrimitives.WriteInt32LittleEndian(buf, i);
         while (peer.TrySend(62, buf) == DatagramSendResult.QueueFull)
         {
            Pump(hub, client, server, ticks: 1, stepMs: 16, onServerEvent: evt =>
            {
               if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 62) ch62Count++;
            });
         }
         if (i % 64 == 0)
         {
            Pump(hub, client, server, ticks: 1, stepMs: 5, onServerEvent: evt =>
            {
               if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 62) ch62Count++;
            });
         }
      }

      for (var tick = 0; tick < 200 && ch62Count < wrapTotal; tick++)
      {
         Pump(hub, client, server, ticks: 1, stepMs: 10, onServerEvent: evt =>
         {
            if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 62) ch62Count++;
         });
      }

      await Assert.That(ch62Count).IsEqualTo(wrapTotal);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }

   [Test]
   public async Task ConcurrentMultiThreadSends_WhileTicking_DeliversAllReliableMessagesCleanly()
   {
      var hub = new SimulatedNetworkHub(seed: 504);
      var (server, client, clientPeer, _) = await ConnectPairAsync(hub, CreateAllModesOptions(reliableWindow: 512, maxQueue: 2048), CreateAllModesOptions(reliableWindow: 512, maxQueue: 2048));

      const int threads = 4;
      const int perThread = 100;

      // 4 producer threads hammer Channel 1 (ReliableUnordered) and Channel 2 (UnreliableSequenced) concurrently!
      var tasks = Enumerable.Range(0, threads).Select(tIdx => Task.Run(() =>
      {
         var localBuf = new byte[4];
         for (var i = 0; i < perThread; i++)
         {
            BinaryPrimitives.WriteInt32LittleEndian(localBuf, tIdx * 1000 + i);
            while (clientPeer.TrySend(1, localBuf) == DatagramSendResult.QueueFull)
            {
               Thread.Yield();
            }
            clientPeer.TrySend(2, localBuf);
         }
      })).ToArray();

      var receivedUnordered = new HashSet<int>();
      while (!Task.WhenAll(tasks).IsCompleted)
      {
         Pump(hub, client, server, ticks: 2, stepMs: 10, onServerEvent: evt =>
         {
            if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 1)
               receivedUnordered.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
         });
      }
      await Task.WhenAll(tasks);

      for (var i = 0; i < 150 && receivedUnordered.Count < threads * perThread; i++)
      {
         Pump(hub, client, server, ticks: 1, stepMs: 15, onServerEvent: evt =>
         {
            if (evt.Type == DatagramEventType.MessageReceived && evt.ChannelId == 1)
               receivedUnordered.Add(BinaryPrimitives.ReadInt32LittleEndian(evt.Payload.Span));
         });
      }

      await Assert.That(receivedUnordered.Count).IsEqualTo(threads * perThread);

      await client.DisposeAsync();
      await server.DisposeAsync();
   }
}
