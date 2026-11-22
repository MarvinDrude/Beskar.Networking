using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Options;
using Beskar.Networking.Datagram.Protocol;
using Beskar.Networking.Datagram.Transport;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Datagram.Nat;

/// <summary>
/// Lightweight UDP rendezvous and endpoint-discovery server for NAT hole punching.
/// Stores observed public IP:Port mappings per node ID with TTL expiry and bounded capacity,
/// and coordinates simultaneous peer introductions.
/// </summary>
public sealed class DatagramRendezvousServer(
   IPEndPoint bindEndPoint,
   IDatagramTransport? transport = null,
   DatagramOptions? options = null,
   TimeProvider? timeProvider = null)
   : IAsyncDisposable
{
   private sealed record PeerRegistration(
      byte[] NodeId,
      IPEndPoint PublicEndPoint,
      IPEndPoint? LanEndPoint,
      long LastSeenTimestamp);

   private readonly IPEndPoint _bindEndPoint = bindEndPoint;
   private readonly IDatagramTransport _transport = transport ?? new UdpSocketDatagramTransport();
   private readonly DatagramOptions _options = options ?? new DatagramOptions();
   private readonly TimeProvider _timeProvider = timeProvider ?? TimeProvider.System;
   private readonly ConcurrentDictionary<string, PeerRegistration> _registrations = new(StringComparer.Ordinal);

   private readonly Lock _evictionLock = new();
   private readonly CancellationTokenSource _cts = new();

   private int _maxRegistrations = 16_384;
   private TimeSpan _registrationTtl = TimeSpan.FromMinutes(2);
   private int _registerOpsSincePurge;

   private Task? _receiveTask;
   private int _started;
   private int _disposed;

   /// <summary>
   /// Gets the bound local endpoint once <see cref="Start"/> has been called.
   /// </summary>
   public IPEndPoint? LocalEndPoint => _transport.LocalEndPoint;

   /// <summary>
   /// Gets the current number of active peer registrations held in memory.
   /// </summary>
   public int RegistrationCount => _registrations.Count;

   /// <summary>
   /// Maximum number of concurrent node registrations retained in memory before evicting expired or oldest entries.
   /// Defaults to <c>16384</c>.
   /// </summary>
   public int MaxRegistrations
   {
      get => Volatile.Read(ref _maxRegistrations);
      set
      {
         if (value < 1)
         {
            throw new ArgumentOutOfRangeException(nameof(value), "MaxRegistrations must be at least 1.");
         }

         Volatile.Write(ref _maxRegistrations, value);
      }
   }

   /// <summary>
   /// Maximum age of an idle peer registration before it is considered expired and evicted.
   /// Defaults to 2 minutes.
   /// </summary>
   public TimeSpan RegistrationTtl
   {
      get => _registrationTtl;
      set
      {
         if (value <= TimeSpan.Zero)
         {
            throw new ArgumentOutOfRangeException(nameof(value), "RegistrationTtl must be positive.");
         }

         _registrationTtl = value;
      }
   }

   /// <summary>
   /// Binds the underlying UDP transport and starts processing rendezvous packets. Safe to call multiple times.
   /// </summary>
   public void Start()
   {
      ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);

      if (Interlocked.CompareExchange(ref _started, 1, 0) != 0)
      {
         return;
      }

      _transport.Bind(_bindEndPoint, _options);
      _receiveTask = Task.Run(() => ReceiveLoopAsync(_cts.Token));
      TraceLogger.LogServerInfo("DatagramRendezvousServer started on {0}", _transport.LocalEndPoint);
   }

   /// <summary>
   /// Removes all peer registrations older than <see cref="RegistrationTtl"/> and returns the number purged.
   /// </summary>
   public int PurgeExpiredRegistrations()
   {
      var now = _timeProvider.GetTimestamp();
      var ttlTicks = GetRegistrationTtlTicks();
      var removed = 0;

      foreach (var kvp in _registrations)
      {
         if (now - kvp.Value.LastSeenTimestamp >= ttlTicks
             && _registrations.TryRemove(kvp))
         {
            removed++;
         }
      }

      return removed;
   }

   private async Task ReceiveLoopAsync(CancellationToken ct)
   {
      var buf = ArrayPool<byte>.Shared.Rent(2048);
      var remoteSa = new SocketAddress(_bindEndPoint.AddressFamily);

      try
      {
         while (!ct.IsCancellationRequested && Volatile.Read(ref _disposed) == 0)
         {
            int received;
            try
            {
               received = await _transport.ReceiveFromAsync(buf, remoteSa, ct);
            }
            catch (SocketException ex) when (ex.SocketErrorCode is SocketError.ConnectionReset or SocketError.MessageSize)
            {
               continue;
            }

            if (received < DatagramWireHeader.Size)
            {
               continue;
            }

            if (TryHandlePacket(
                   buf.AsSpan(0, received),
                   remoteSa,
                   out var send1,
                   out var send2))
            {
               if (send1.Buffer is not null)
               {
                  await SendAndReturnAsync(send1.Buffer, send1.Length, send1.Destination!, ct);
               }
               if (send2.Buffer is not null)
               {
                  await SendAndReturnAsync(send2.Buffer, send2.Length, send2.Destination!, ct);
               }
            }
         }
      }
      catch (OperationCanceledException)
      {
      }
      catch (ObjectDisposedException)
      {
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("DatagramRendezvousServer receive loop error: {0}", ex.Message);
      }
      finally
      {
         ArrayPool<byte>.Shared.Return(buf);
      }
   }

   private readonly record struct OutgoingPacket(byte[]? Buffer, int Length, SocketAddress? Destination);

   private bool TryHandlePacket(
      ReadOnlySpan<byte> rawPacket,
      SocketAddress remoteSa,
      out OutgoingPacket send1,
      out OutgoingPacket send2)
   {
      send1 = default;
      send2 = default;

      if (!TryUnwrapControlPacket(rawPacket, out var header, out var body))
      {
         return false;
      }

      var senderEp = MaterializeEndPoint(remoteSa);

      switch (header.Kind)
      {
         case DatagramPacketKind.RendezvousRegister:
            return TryBuildRegisterReply(senderEp, body, out send1);

         case DatagramPacketKind.RendezvousIntroduce:
            return TryBuildIntroduceNotifications(body, out send1, out send2);

         default:
            return false;
      }
   }

   private bool TryBuildRegisterReply(IPEndPoint senderEp, ReadOnlySpan<byte> body, out OutgoingPacket reply)
   {
      reply = default;
      if (!DatagramRendezvousCodec.TryReadRegister(body, out var nodeIdSpan, out var lanHint))
      {
         return false;
      }

      var key = Convert.ToHexString(nodeIdSpan);
      var nodeIdCopy = nodeIdSpan.ToArray();
      var now = _timeProvider.GetTimestamp();

      if ((Interlocked.Increment(ref _registerOpsSincePurge) & 63) == 0)
      {
         PurgeExpiredRegistrations();
      }

      var maxRegs = Volatile.Read(ref _maxRegistrations);
      if (!_registrations.ContainsKey(key) && _registrations.Count >= maxRegs)
      {
         lock (_evictionLock)
         {
            if (!_registrations.ContainsKey(key) && _registrations.Count >= maxRegs)
            {
               PurgeExpiredRegistrations();
               while (_registrations.Count >= maxRegs)
               {
                  if (!EvictOldestRegistration())
                  {
                     break;
                  }
               }
            }
         }
      }

      _registrations[key] = new PeerRegistration(nodeIdCopy, senderEp, lanHint, now);

      Span<byte> respBody = stackalloc byte[128];
      var bodyLen = DatagramRendezvousCodec.WriteMapped(respBody, nodeIdCopy, senderEp);
      reply = BuildControlPacket(DatagramPacketKind.RendezvousMapped, respBody[0..bodyLen], senderEp.Serialize());
      return true;
   }

   private bool TryBuildIntroduceNotifications(ReadOnlySpan<byte> body, out OutgoingPacket send1, out OutgoingPacket send2)
   {
      send1 = default;
      send2 = default;

      if (!DatagramRendezvousCodec.TryReadIntroduceRequest(body, out var reqId, out var tgtId, out var punchToken))
      {
         return false;
      }

      var reqKey = Convert.ToHexString(reqId);
      var tgtKey = Convert.ToHexString(tgtId);
      if (!_registrations.TryGetValue(reqKey, out var reqReg)
          || !_registrations.TryGetValue(tgtKey, out var tgtReg))
      {
         return false;
      }

      var now = _timeProvider.GetTimestamp();
      var ttlTicks = GetRegistrationTtlTicks();
      var anyExpired = false;

      if (now - reqReg.LastSeenTimestamp >= ttlTicks)
      {
         _registrations.TryRemove(KeyValuePair.Create(reqKey, reqReg));
         anyExpired = true;
      }

      if (now - tgtReg.LastSeenTimestamp >= ttlTicks)
      {
         _registrations.TryRemove(KeyValuePair.Create(tgtKey, tgtReg));
         anyExpired = true;
      }

      if (anyExpired)
      {
         return false;
      }

      Span<byte> msgToReq = stackalloc byte[256];
      var len1 = DatagramRendezvousCodec.WriteIntroduceNotify(
         msgToReq, tgtReg.NodeId, punchToken, tgtReg.PublicEndPoint, tgtReg.LanEndPoint);

      Span<byte> msgToTgt = stackalloc byte[256];
      var len2 = DatagramRendezvousCodec.WriteIntroduceNotify(
         msgToTgt, reqReg.NodeId, punchToken, reqReg.PublicEndPoint, reqReg.LanEndPoint);

      send1 = BuildControlPacket(DatagramPacketKind.RendezvousIntroduce, msgToReq[0..len1], reqReg.PublicEndPoint.Serialize());
      send2 = BuildControlPacket(DatagramPacketKind.RendezvousIntroduce, msgToTgt[0..len2], tgtReg.PublicEndPoint.Serialize());
      return true;
   }

   private bool EvictOldestRegistration()
   {
      string? oldestKey = null;
      PeerRegistration? oldestReg = null;
      var oldestTimestamp = long.MaxValue;

      foreach (var kvp in _registrations)
      {
         if (kvp.Value.LastSeenTimestamp < oldestTimestamp)
         {
            oldestTimestamp = kvp.Value.LastSeenTimestamp;
            oldestKey = kvp.Key;
            oldestReg = kvp.Value;
         }
      }

      return oldestKey is not null
         && oldestReg is not null
         && _registrations.TryRemove(KeyValuePair.Create(oldestKey, oldestReg));
   }

   private long GetRegistrationTtlTicks()
   {
      var ttl = _registrationTtl;
      return Math.Max(1, ttl.Ticks * _timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond);
   }

   private OutgoingPacket BuildControlPacket(
      DatagramPacketKind kind,
      ReadOnlySpan<byte> body,
      SocketAddress destination)
   {
      var useCrc = _options.Security.Mode != DatagramSecurityMode.None;
      var totalLen = DatagramWireHeader.Size + body.Length + (useCrc ? Crc32C.Size : 0);
      var packet = ArrayPool<byte>.Shared.Rent(totalLen);

      var span = packet.AsSpan(0, totalLen);
      var header = new DatagramWireHeader(kind, DatagramWireHeader.CurrentVersion, false, 0, 0, 0, 0);
      header.Write(span[0..DatagramWireHeader.Size]);
      body.CopyTo(span.Slice(DatagramWireHeader.Size, body.Length));

      if (useCrc)
      {
         var crc = Crc32C.Compute(span[0..(DatagramWireHeader.Size + body.Length)]);
         BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(DatagramWireHeader.Size + body.Length, Crc32C.Size), crc);
      }

      return new OutgoingPacket(packet, totalLen, destination);
   }

   private async ValueTask SendAndReturnAsync(byte[] packet, int totalLen, SocketAddress destination, CancellationToken ct)
   {
      try
      {
         await _transport.SendToAsync(packet.AsMemory(0, totalLen), destination, ct);
      }
      finally
      {
         ArrayPool<byte>.Shared.Return(packet);
      }
   }

   private bool TryUnwrapControlPacket(ReadOnlySpan<byte> packet, out DatagramWireHeader header, out ReadOnlySpan<byte> body)
   {
      body = default;
      if (!DatagramWireHeader.TryRead(packet, out header) || header.IsEncrypted)
      {
         return false;
      }

      if (_options.Security.Mode != DatagramSecurityMode.None)
      {
         if (packet.Length < DatagramWireHeader.Size + Crc32C.Size) return false;
         var payloadEnd = packet.Length - Crc32C.Size;
         var computed = Crc32C.Compute(packet[0..payloadEnd]);
         var wireCrc = BinaryPrimitives.ReadUInt32LittleEndian(packet.Slice(payloadEnd, Crc32C.Size));
         if (computed != wireCrc) return false;
         body = packet[DatagramWireHeader.Size..payloadEnd];
      }
      else
      {
         body = packet[DatagramWireHeader.Size..];
      }

      return true;
   }

   private static IPEndPoint MaterializeEndPoint(SocketAddress sa)
   {
      var template = sa.Family == AddressFamily.InterNetworkV6
         ? new IPEndPoint(IPAddress.IPv6Any, 0)
         : new IPEndPoint(IPAddress.Any, 0);

      return (IPEndPoint)template.Create(sa);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1) return;

      await _cts.CancelAsync();
      await _transport.DisposeAsync();

      if (_receiveTask is not null)
      {
         try
         {
            await _receiveTask;
         }
         catch
         {
            // ignored
         }
      }

      _cts.Dispose();
   }
}
