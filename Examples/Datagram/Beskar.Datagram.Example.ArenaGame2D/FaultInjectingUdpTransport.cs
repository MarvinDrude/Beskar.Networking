using System.Diagnostics;
using System.Net;
using System.Threading.Channels;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Transport;

namespace Beskar.Datagram.Example.ArenaGame2D;

public sealed class FaultInjectingUdpTransport : IDatagramTransport
{
   private readonly record struct DelayedPacket(byte[] Data, SocketAddress Address, long DueTimestamp);

   private readonly UdpSocketDatagramTransport _inner = new();
   private readonly Lock _outLock = new();
   private readonly Lock _inLock = new();
   private readonly PriorityQueue<DelayedPacket, long> _outboundDelayed = new();
   private readonly PriorityQueue<DelayedPacket, long> _inboundDelayed = new();
   private readonly Channel<(byte[] Data, SocketAddress Source)> _inboundReady = Channel.CreateUnbounded<(byte[], SocketAddress)>(
      new UnboundedChannelOptions { SingleReader = true, SingleWriter = false });

   private CancellationTokenSource? _cts;
   private Task? _pumpTask;
   private Task? _recvTask;
   private int _disposed;

   private int _addedLatencyMs;
   private int _jitterMs;
   private int _packetLossPercent;

   private long _droppedPackets;
   private long _delayedPacketsTotal;

   public IPEndPoint? LocalEndPoint => _inner.LocalEndPoint;

   public int AddedLatencyMs
   {
      get => Volatile.Read(ref _addedLatencyMs);
      set => Volatile.Write(ref _addedLatencyMs, Math.Clamp(value, 0, 500));
   }

   public int JitterMs
   {
      get => Volatile.Read(ref _jitterMs);
      set => Volatile.Write(ref _jitterMs, Math.Clamp(value, 0, 200));
   }

   public int PacketLossPercent
   {
      get => Volatile.Read(ref _packetLossPercent);
      set => Volatile.Write(ref _packetLossPercent, Math.Clamp(value, 0, 50));
   }

   public long DroppedPackets => Interlocked.Read(ref _droppedPackets);
   public long DelayedPacketsTotal => Interlocked.Read(ref _delayedPacketsTotal);

   public void Bind(IPEndPoint localEndPoint, DatagramOptions options)
   {
      _inner.Bind(localEndPoint, options);
      _cts = new CancellationTokenSource();
      _recvTask = Task.Run(() => InnerReceivePumpAsync(_cts.Token));
      _pumpTask = Task.Run(() => DelaySchedulerLoopAsync(_cts.Token));
   }

   public async ValueTask<int> ReceiveFromAsync(Memory<byte> buffer, SocketAddress remoteAddress, CancellationToken ct)
   {
      var (data, src) = await _inboundReady.Reader.ReadAsync(ct);
      var len = Math.Min(data.Length, buffer.Length);
      data.AsSpan(0, len).CopyTo(buffer.Span);
      src.Buffer.Span[0..src.Size].CopyTo(remoteAddress.Buffer.Span);
      remoteAddress.Size = src.Size;
      return len;
   }

   public ValueTask SendToAsync(ReadOnlyMemory<byte> datagram, SocketAddress remoteAddress, CancellationToken ct)
   {
      if (Volatile.Read(ref _disposed) == 1)
      {
         return ValueTask.CompletedTask;
      }

      if (ShouldDrop())
      {
         Interlocked.Increment(ref _droppedPackets);
         return ValueTask.CompletedTask;
      }

      var delayMs = ComputeDelayMs();
      if (delayMs <= 0)
      {
         return _inner.SendToAsync(datagram, remoteAddress, ct);
      }

      var copy = datagram.ToArray();
      var addrCopy = CloneSocketAddress(remoteAddress);
      var due = Stopwatch.GetTimestamp() + (long)(delayMs * (Stopwatch.Frequency / 1000.0));

      lock (_outLock)
      {
         _outboundDelayed.Enqueue(new DelayedPacket(copy, addrCopy, due), due);
      }

      Interlocked.Increment(ref _delayedPacketsTotal);
      return ValueTask.CompletedTask;
   }

   private async Task InnerReceivePumpAsync(CancellationToken ct)
   {
      var buf = new byte[4096];
      var family = _inner.LocalEndPoint?.AddressFamily ?? System.Net.Sockets.AddressFamily.InterNetwork;
      var sa = new SocketAddress(family);

      try
      {
         while (!ct.IsCancellationRequested)
         {
            int n;
            try
            {
               n = await _inner.ReceiveFromAsync(buf, sa, ct);
            }
            catch (System.Net.Sockets.SocketException ex)
               when (ex.SocketErrorCode is System.Net.Sockets.SocketError.ConnectionReset or System.Net.Sockets.SocketError.MessageSize)
            {
               continue;
            }

            if (n <= 0) continue;

            if (ShouldDrop())
            {
               Interlocked.Increment(ref _droppedPackets);
               continue;
            }

            var data = buf.AsSpan(0, n).ToArray();
            var addrCopy = CloneSocketAddress(sa);
            var delayMs = ComputeDelayMs();

            if (delayMs <= 0)
            {
               _inboundReady.Writer.TryWrite((data, addrCopy));
            }
            else
            {
               var due = Stopwatch.GetTimestamp() + (long)(delayMs * (Stopwatch.Frequency / 1000.0));
               lock (_inLock)
               {
                  _inboundDelayed.Enqueue(new DelayedPacket(data, addrCopy, due), due);
               }
               Interlocked.Increment(ref _delayedPacketsTotal);
            }
         }
      }
      catch (OperationCanceledException) { }
      catch (ObjectDisposedException) { }
      finally
      {
         _inboundReady.Writer.TryComplete();
      }
   }

   private async Task DelaySchedulerLoopAsync(CancellationToken ct)
   {
      using var timer = new PeriodicTimer(TimeSpan.FromMilliseconds(4));
      try
      {
         while (await timer.WaitForNextTickAsync(ct))
         {
            var now = Stopwatch.GetTimestamp();

            // Flush due outbound packets
            while (true)
            {
               DelayedPacket pkt;
               lock (_outLock)
               {
                  if (!_outboundDelayed.TryPeek(out pkt, out _) || pkt.DueTimestamp > now) break;
                  _outboundDelayed.Dequeue();
               }

               try
               {
                  await _inner.SendToAsync(pkt.Data, pkt.Address, ct);
               }
               catch
               {
               }
            }

            // Flush due inbound packets
            while (true)
            {
               DelayedPacket pkt;
               lock (_inLock)
               {
                  if (!_inboundDelayed.TryPeek(out pkt, out _) || pkt.DueTimestamp > now) break;
                  _inboundDelayed.Dequeue();
               }

               _inboundReady.Writer.TryWrite((pkt.Data, pkt.Address));
            }
         }
      }
      catch (OperationCanceledException) { }
   }

   private bool ShouldDrop()
   {
      var loss = Volatile.Read(ref _packetLossPercent);
      return loss > 0 && Random.Shared.NextDouble() * 100.0 < loss;
   }

   private int ComputeDelayMs()
   {
      var baseMs = Volatile.Read(ref _addedLatencyMs);
      var jitter = Volatile.Read(ref _jitterMs);
      if (baseMs <= 0 && jitter <= 0) return 0;

      var offset = jitter > 0 ? Random.Shared.Next(-jitter, jitter + 1) : 0;
      return Math.Max(0, baseMs + offset);
   }

   private static SocketAddress CloneSocketAddress(SocketAddress src)
   {
      var copy = new SocketAddress(src.Family, src.Size);
      src.Buffer.Span[0..src.Size].CopyTo(copy.Buffer.Span);
      copy.Size = src.Size;
      return copy;
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

      if (_cts is not null) await _cts.CancelAsync();
      _inboundReady.Writer.TryComplete();
      await _inner.DisposeAsync();

      if (_recvTask is not null) { try { await _recvTask; } catch { } }
      if (_pumpTask is not null) { try { await _pumpTask; } catch { } }
      _cts?.Dispose();
   }
}
