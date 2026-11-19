using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Threading.Channels;
using Beskar.Networking.Datagram.Options;

namespace Beskar.Networking.Datagram.Transport;

/// <summary>
/// Configurable network fault-injection profile for <see cref="SimulatedNetworkHub"/>.
/// </summary>
public sealed class SimulatedChaosProfile
{
   public double PacketDropRate { get; set; }
   public double PacketDuplicateRate { get; set; }
   public double DataCorruptionRate { get; set; }
   public double ReorderRate { get; set; }

   public TimeSpan MinLatency { get; set; } = TimeSpan.Zero;
   public TimeSpan MaxLatency { get; set; } = TimeSpan.Zero;
   public TimeSpan ReorderExtraDelay { get; set; } = TimeSpan.FromMilliseconds(35);

   public static SimulatedChaosProfile Clean => new();
}

/// <summary>
/// Controllable <see cref="TimeProvider"/> for deterministic virtual-clock simulation.
/// </summary>
public sealed class SimulatedTimeProvider(DateTimeOffset? startTime = null) : TimeProvider
{
   private long _utcTicks = (startTime ?? new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero)).UtcTicks;

   public override DateTimeOffset GetUtcNow() => new(Interlocked.Read(ref _utcTicks), TimeSpan.Zero);
   public override long GetTimestamp() => Interlocked.Read(ref _utcTicks);

   public override long TimestampFrequency => TimeSpan.TicksPerSecond;

   public void Advance(TimeSpan delta)
   {
      if (delta < TimeSpan.Zero)
         throw new ArgumentOutOfRangeException(nameof(delta));

      Interlocked.Add(ref _utcTicks, delta.Ticks);
   }
}

/// <summary>
/// In-memory virtual UDP network fabric supporting deterministic packet loss, jitter, reordering,
/// duplication, corruption, and simulated NAT endpoint rebinding.
/// </summary>
public sealed class SimulatedNetworkHub(
   int seed = 1337,
   SimulatedChaosProfile? chaos = null,
   SimulatedTimeProvider? time = null)
{
   private readonly record struct QueuedPacket(
      long DeliverAtTimestamp,
      long SequenceOrder,
      IPEndPoint Source,
      IPEndPoint Destination,
      byte[] Payload);

   private readonly ConcurrentDictionary<IPEndPoint, SimulatedDatagramTransport> _nodes = new();
   private readonly Lock _queueLock = new();
   private readonly PriorityQueue<QueuedPacket, (long DeliverAt, long Order)> _inFlight = new();
   private readonly Random _rng = new(seed);

   private long _nextSequence;
   private int _nextEphemeralPort = 40000;

   public SimulatedTimeProvider Time { get; } = time ?? new SimulatedTimeProvider();
   public SimulatedChaosProfile Chaos { get; set; } = chaos ?? new SimulatedChaosProfile();

   /// <summary>
   /// Optional interceptor invoked for every sent packet before chaos rules run.
   /// Return <c>false</c> to deterministically drop the packet, or <c>true</c> to allow it (with any in-place mutations).
   /// </summary>
   public Func<IPEndPoint, IPEndPoint, byte[], bool>? OnPacketSending { get; set; }

   public SimulatedDatagramTransport CreateTransport() => new(this);

   /// <summary>
   /// Directly injects a raw datagram from an arbitrary source endpoint (for replay, spoofing, or reordering tests).
   /// </summary>
   public void InjectRawPacket(IPEndPoint fakeSource, IPEndPoint destination, byte[] rawPayload, TimeSpan? delay = null)
   {
      var copy = (byte[])rawPayload.Clone();
      var delayTicks = delay?.Ticks ?? 0;

      if (delayTicks <= 0)
      {
         if (_nodes.TryGetValue(destination, out var target))
         {
            target.EnqueueIncoming(fakeSource, copy);
         }
         return;
      }

      lock (_queueLock)
      {
         var deliverAt = Time.GetTimestamp() + delayTicks;
         var order = ++_nextSequence;
         _inFlight.Enqueue(new QueuedPacket(deliverAt, order, fakeSource, destination, copy), (deliverAt, order));
      }
   }

   internal IPEndPoint Register(SimulatedDatagramTransport transport, IPEndPoint requestedEndPoint)
   {
      var ep = requestedEndPoint;
      if (ep.Port == 0)
      {
         var port = Interlocked.Increment(ref _nextEphemeralPort);
         var addr = ep.Address.Equals(IPAddress.Any) ? IPAddress.Loopback : ep.Address;
         ep = new IPEndPoint(addr, port);
      }
      else if (ep.Address.Equals(IPAddress.Any))
      {
         ep = new IPEndPoint(IPAddress.Loopback, ep.Port);
      }

      if (!_nodes.TryAdd(ep, transport))
      {
         throw new InvalidOperationException($"Endpoint {ep} is already bound in SimulatedNetworkHub.");
      }

      return ep;
   }

   internal void Unregister(IPEndPoint? ep)
   {
      if (ep is not null)
      {
         _nodes.TryRemove(ep, out _);
      }
   }

   /// <summary>
   /// Rebinds a transport to a new <see cref="IPEndPoint"/> to simulate NAT port/IP migration.
   /// </summary>
   public IPEndPoint MigrateEndpoint(SimulatedDatagramTransport transport, IPEndPoint newEndPoint)
   {
      var oldEp = transport.LocalEndPoint;
      var bound = Register(transport, newEndPoint);

      if (oldEp is not null)
      {
         _nodes.TryRemove(oldEp, out _);
      }

      transport.UpdateLocalEndPoint(bound);
      return bound;
   }

   internal void Send(IPEndPoint source, IPEndPoint destination, ReadOnlySpan<byte> datagram)
   {
      SimulatedDatagramTransport? directTarget = null;
      byte[]? directCopy = null;

      lock (_queueLock)
      {
         var copy = datagram.ToArray();

         if (OnPacketSending is not null && !OnPacketSending(source, destination, copy))
         {
            return;
         }

         if (Chaos.PacketDropRate > 0 && _rng.NextDouble() < Chaos.PacketDropRate)
         {
            return;
         }

         if (Chaos.DataCorruptionRate > 0 && copy.Length > 0 && _rng.NextDouble() < Chaos.DataCorruptionRate)
         {
            copy[_rng.Next(copy.Length)] ^= (byte)(1 << _rng.Next(8));
         }

         var delayTicks = ComputeDelayTicks();
         if (delayTicks == 0 && (Chaos.PacketDuplicateRate <= 0 || _rng.NextDouble() >= Chaos.PacketDuplicateRate))
         {
            if (_nodes.TryGetValue(destination, out directTarget))
            {
               directCopy = copy;
            }
         }

         if (directTarget is null)
         {
            var deliverAt = Time.GetTimestamp() + delayTicks;
            var order = ++_nextSequence;
            _inFlight.Enqueue(new QueuedPacket(deliverAt, order, source, destination, copy), (deliverAt, order));

            if (Chaos.PacketDuplicateRate > 0 && _rng.NextDouble() < Chaos.PacketDuplicateRate)
            {
               var dupExtra = TimeSpan.FromMilliseconds(_rng.Next(1, 15)).Ticks;
               var dupDeliver = deliverAt + dupExtra;
               var dupOrder = ++_nextSequence;
               var dupCopy = (byte[])copy.Clone();

               _inFlight.Enqueue(new QueuedPacket(dupDeliver, dupOrder, source, destination, dupCopy), (dupDeliver, dupOrder));
            }
         }
      }

      if (directTarget is not null && directCopy is not null)
      {
         directTarget.EnqueueIncoming(source, directCopy);
      }
   }

   /// <summary>
   /// Advances virtual time by <paramref name="delta"/> and flushes all due packets to destination nodes.
   /// </summary>
   public void AdvanceAndDeliver(TimeSpan delta)
   {
      Time.Advance(delta);
      DeliverDue();
   }

   /// <summary>
   /// Delivers all packets scheduled on or before the current virtual timestamp.
   /// </summary>
   public void DeliverDue()
   {
      var now = Time.GetTimestamp();
      while (true)
      {
         QueuedPacket pkt;
         lock (_queueLock)
         {
            if (!_inFlight.TryPeek(out pkt, out _) || pkt.DeliverAtTimestamp > now)
            {
               break;
            }
            _inFlight.Dequeue();
         }

         if (_nodes.TryGetValue(pkt.Destination, out var target))
         {
            target.EnqueueIncoming(pkt.Source, pkt.Payload);
         }
      }
   }

   private long ComputeDelayTicks()
   {
      var minTicks = Chaos.MinLatency.Ticks;
      var maxTicks = Math.Max(minTicks, Chaos.MaxLatency.Ticks);
      long ticks = minTicks;

      if (maxTicks > minTicks)
      {
         ticks += (long)(_rng.NextDouble() * (maxTicks - minTicks));
      }

      if (Chaos.ReorderRate > 0 && _rng.NextDouble() < Chaos.ReorderRate)
      {
         ticks += Chaos.ReorderExtraDelay.Ticks;
      }

      return ticks;
   }
}

/// <summary>
/// In-memory <see cref="IDatagramTransport"/> bound to a <see cref="SimulatedNetworkHub"/>.
/// </summary>
public sealed class SimulatedDatagramTransport : IDatagramTransport
{
   private readonly record struct IncomingDatagram(IPEndPoint Source, byte[] Payload);

   private readonly SimulatedNetworkHub _hub;
   private readonly Channel<IncomingDatagram> _inbox = Channel.CreateUnbounded<IncomingDatagram>(
      new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

   private IPEndPoint? _localEndPoint;
   private int _disposed;

   internal SimulatedDatagramTransport(SimulatedNetworkHub hub)
   {
      _hub = hub;
   }

   public IPEndPoint? LocalEndPoint => _localEndPoint;

   public void Bind(IPEndPoint localEndPoint, DatagramOptions options)
   {
      _localEndPoint = _hub.Register(this, localEndPoint);
   }

   internal void UpdateLocalEndPoint(IPEndPoint newEndPoint) => _localEndPoint = newEndPoint;

   internal void EnqueueIncoming(IPEndPoint source, byte[] payload)
   {
      if (Volatile.Read(ref _disposed) == 0)
      {
         _inbox.Writer.TryWrite(new IncomingDatagram(source, payload));
      }
   }

   public async ValueTask<int> ReceiveFromAsync(Memory<byte> buffer, SocketAddress remoteAddress, CancellationToken ct)
   {
      var msg = await _inbox.Reader.ReadAsync(ct);
      return CopyIncoming(msg, buffer.Span, remoteAddress);
   }

   internal bool TryDequeueImmediate(Span<byte> buffer, SocketAddress remoteAddress, out int bytesRead)
   {
      if (_inbox.Reader.TryRead(out var msg))
      {
         bytesRead = CopyIncoming(msg, buffer, remoteAddress);
         return true;
      }

      bytesRead = 0;
      return false;
   }

   private static int CopyIncoming(IncomingDatagram msg, Span<byte> buffer, SocketAddress remoteAddress)
   {
      var len = Math.Min(msg.Payload.Length, buffer.Length);
      msg.Payload.AsSpan(0, len).CopyTo(buffer);

      var serialized = msg.Source.Serialize();
      serialized.Buffer.Span.CopyTo(remoteAddress.Buffer.Span);
      remoteAddress.Size = serialized.Size;

      return len;
   }

   public ValueTask SendToAsync(ReadOnlyMemory<byte> datagram, SocketAddress remoteAddress, CancellationToken ct)
   {
      if (Volatile.Read(ref _disposed) == 1 || _localEndPoint is null)
      {
         return ValueTask.CompletedTask;
      }

      var template = remoteAddress.Family == AddressFamily.InterNetworkV6
         ? new IPEndPoint(IPAddress.IPv6Any, 0)
         : new IPEndPoint(IPAddress.Any, 0);

      var dest = (IPEndPoint)template.Create(remoteAddress);

      _hub.Send(_localEndPoint, dest, datagram.Span);
      return ValueTask.CompletedTask;
   }

   public ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return ValueTask.CompletedTask;
      }

      _hub.Unregister(_localEndPoint);
      _inbox.Writer.TryComplete();
      return ValueTask.CompletedTask;
   }
}
