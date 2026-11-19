using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Nat;
using Beskar.Networking.Datagram.Protocol;

namespace Beskar.Networking.Datagram;

public sealed partial class DatagramHost
{
   private sealed class PendingDiscovery
   {
      public required byte[] NodeId { get; init; }
      public required TaskCompletionSource<IPEndPoint?> CompletionSource { get; init; }
   }

   private sealed class ActivePunchSession
   {
      public required ulong PunchToken { get; init; }
      public required IPEndPoint PublicTarget { get; init; }
      public required SocketAddress PublicAddress { get; init; }
      public IPEndPoint? LanTarget { get; init; }
      public SocketAddress? LanAddress { get; init; }
      public required long StartedTimestamp { get; init; }
      public required long DeadlineTimestamp { get; init; }
      public required long ProbeIntervalTicks { get; init; }
      public long NextProbeTimestamp { get; set; }
      public long LastProbeSentTimestamp { get; set; }
      public required TaskCompletionSource<DatagramPunchResult> CompletionSource { get; init; }
      public int Completed;
   }

   private readonly ConcurrentDictionary<string, PendingDiscovery> _pendingDiscoveries = new(StringComparer.Ordinal);
   private readonly ConcurrentDictionary<ulong, ActivePunchSession> _activePunches = new();
   private readonly ConcurrentDictionary<ulong, long> _recentPunchTokens = new();

   /// <summary>
   /// Registers this host with a <see cref="DatagramRendezvousServer"/> over its bound socket and
   /// discovers its NAT-mapped public <see cref="IPEndPoint"/>.
   /// </summary>
   public async ValueTask<IPEndPoint?> DiscoverPublicEndPointAsync(
      IPEndPoint rendezvousServer,
      ReadOnlyMemory<byte> nodeId,
      IPEndPoint? lanHint = null,
      TimeSpan? timeout = null,
      CancellationToken ct = default)
   {
      if (nodeId.IsEmpty || nodeId.Length > 64)
      {
         throw new ArgumentOutOfRangeException(nameof(nodeId), "NodeId must be 1..64 bytes.");
      }

      if (Volatile.Read(ref _started) == 0)
      {
         Start();
      }

      var key = Convert.ToHexString(nodeId.Span);
      var pending = new PendingDiscovery
      {
         NodeId = nodeId.ToArray(),
         CompletionSource = new TaskCompletionSource<IPEndPoint?>(TaskCreationOptions.RunContinuationsAsynchronously)
      };

      _pendingDiscoveries[key] = pending;

      using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      timeoutCts.CancelAfter(timeout ?? TimeSpan.FromSeconds(2));
      await using var reg = timeoutCts.Token.Register(() =>
      {
         if (_pendingDiscoveries.TryRemove(key, out var p))
         {
            p.CompletionSource.TrySetResult(null);
         }
      });

      Span<byte> body = stackalloc byte[128];
      var bodyLen = DatagramRendezvousCodec.WriteRegister(body, nodeId.Span, lanHint ?? LocalEndPoint);
      var (pkt, len) = BuildUnencryptedControlPacket(DatagramPacketKind.RendezvousRegister, body[0..bodyLen]);
      DispatchSendAndReturnBuffer(pkt, len, rendezvousServer.Serialize());

      return await pending.CompletionSource.Task;
   }

   /// <summary>
   /// Requests a <see cref="DatagramRendezvousServer"/> to simultaneously introduce this node and <paramref name="targetNodeId"/>.
   /// </summary>
   public void RequestIntroduction(
      IPEndPoint rendezvousServer,
      ReadOnlySpan<byte> myNodeId,
      ReadOnlySpan<byte> targetNodeId,
      ulong punchToken)
   {
      if (Volatile.Read(ref _started) == 0)
      {
         Start();
      }

      Span<byte> body = stackalloc byte[160];
      var bodyLen = DatagramRendezvousCodec.WriteIntroduceRequest(body, myNodeId, targetNodeId, punchToken);
      var (pkt, len) = BuildUnencryptedControlPacket(DatagramPacketKind.RendezvousIntroduce, body[..bodyLen]);

      DispatchSendAndReturnBuffer(pkt, len, rendezvousServer.Serialize());
   }

   /// <summary>
   /// Performs single-socket UDP NAT hole punching toward <paramref name="peerPublicEndPoint"/>
   /// (and optional <paramref name="peerLanEndPoint"/>) using <paramref name="punchToken"/>.
   /// </summary>
   public async ValueTask<DatagramPunchResult> PunchAsync(
      IPEndPoint peerPublicEndPoint,
      IPEndPoint? peerLanEndPoint,
      ulong punchToken,
      DatagramPunchOptions? options = null,
      CancellationToken ct = default)
   {
      if (Volatile.Read(ref _started) == 0)
      {
         Start();
      }

      options ??= new DatagramPunchOptions();

      var now = _timeProvider.GetTimestamp();
      var intervalTicks = Math.Max(1, options.ProbeInterval.Ticks * _timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond);
      var timeoutTicks = Math.Max(1, options.Timeout.Ticks * _timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond);

      var session = new ActivePunchSession
      {
         PunchToken = punchToken,
         PublicTarget = peerPublicEndPoint,
         PublicAddress = peerPublicEndPoint.Serialize(),
         LanTarget = peerLanEndPoint,
         LanAddress = peerLanEndPoint?.Serialize(),
         StartedTimestamp = now,
         DeadlineTimestamp = now + timeoutTicks,
         ProbeIntervalTicks = intervalTicks,
         NextProbeTimestamp = now + intervalTicks,
         LastProbeSentTimestamp = now,
         CompletionSource = new TaskCompletionSource<DatagramPunchResult>(TaskCreationOptions.RunContinuationsAsynchronously)
      };

      if (!_activePunches.TryAdd(punchToken, session))
      {
         throw new InvalidOperationException($"Punch session for token {punchToken} is already active.");
      }

      await using var reg = ct.Register(() =>
      {
         if (_activePunches.TryRemove(punchToken, out var removed)
             && Interlocked.Exchange(ref removed.Completed, 1) == 0)
         {
            removed.CompletionSource.TrySetCanceled(ct);
         }
      });

      SendPunchProbes(session, isReply: false);
      return await session.CompletionSource.Task;
   }

   /// <summary>
   /// Runs <see cref="PunchAsync"/> and immediately opens an authenticated handshake over the punched NAT mapping.
   /// </summary>
   public async Task<DatagramPeer?> PunchAndConnectAsync(
      IPEndPoint peerPublicEndPoint,
      IPEndPoint? peerLanEndPoint,
      ulong punchToken,
      ReadOnlyMemory<byte> connectMetadata = default,
      DatagramPunchOptions? punchOptions = null,
      CancellationToken ct = default)
   {
      var punch = await PunchAsync(peerPublicEndPoint, peerLanEndPoint, punchToken, punchOptions, ct);
      if (!punch.Succeeded || punch.ResolvedEndPoint is null)
      {
         return null;
      }

      return await ConnectAsync(punch.ResolvedEndPoint, connectMetadata, ct);
   }

   private void HandlePunchProbe(ReadOnlySpan<byte> rawPacket, SocketAddress remoteSa)
   {
      if (!TryVerifyHandshakeCrc(rawPacket, out var body))
      {
         return;
      }

      if (!DatagramRendezvousCodec.TryReadPunchProbe(body, out var punchToken, out var isReply))
      {
         return;
      }

      if (!_activePunches.TryGetValue(punchToken, out var session))
      {
         // allow trailing probes from the remote peer right after we finished our side of the punch
         if (!isReply && _recentPunchTokens.TryGetValue(punchToken, out var expiresAt) && _timeProvider.GetTimestamp() < expiresAt)
         {
            var trailingSender = MaterializeEndPoint(remoteSa);
            Span<byte> trailingReply = stackalloc byte[9];
            DatagramRendezvousCodec.WritePunchProbe(trailingReply, punchToken, isReply: true);

            var (tpkt, tlen) = BuildUnencryptedControlPacket(DatagramPacketKind.PunchProbe, trailingReply);
            DispatchSendAndReturnBuffer(tpkt, tlen, trailingSender.Serialize());
         }
         return;
      }

      var senderEp = MaterializeEndPoint(remoteSa);

      if (!isReply)
      {
         Span<byte> replyBody = stackalloc byte[9];
         DatagramRendezvousCodec.WritePunchProbe(replyBody, punchToken, isReply: true);

         var (pkt, len) = BuildUnencryptedControlPacket(DatagramPacketKind.PunchProbe, replyBody);
         DispatchSendAndReturnBuffer(pkt, len, senderEp.Serialize());

         return;
      }

      if (_activePunches.TryRemove(punchToken, out _) && Interlocked.Exchange(ref session.Completed, 1) == 0)
      {
         var now = _timeProvider.GetTimestamp();
         _recentPunchTokens[punchToken] = now + (3 * _timeProvider.TimestampFrequency);
         var rtt = _timeProvider.GetElapsedTime(session.LastProbeSentTimestamp, now);

         session.CompletionSource.TrySetResult(new DatagramPunchResult(
            Succeeded: true,
            ResolvedEndPoint: senderEp,
            RoundTripTime: rtt,
            NeedsRelayFallback: false));
      }
   }

   private void HandleRendezvousPacket(in DatagramWireHeader header, ReadOnlySpan<byte> rawPacket)
   {
      if (!TryVerifyHandshakeCrc(rawPacket, out var body))
      {
         return;
      }

      if (header.Kind == DatagramPacketKind.RendezvousMapped)
      {
         if (DatagramRendezvousCodec.TryReadMapped(body, out var nodeId, out var publicEp))
         {
            var key = Convert.ToHexString(nodeId);
            if (_pendingDiscoveries.TryRemove(key, out var pending))
            {
               pending.CompletionSource.TrySetResult(publicEp);
            }
         }
         return;
      }

      if (header.Kind == DatagramPacketKind.RendezvousIntroduce
          && DatagramRendezvousCodec.TryReadIntroduceNotify(body, out var peerNodeId, out var punchToken, out var peerPublicEp, out var peerLanEp)
          && peerPublicEp is not null
          && Events.PunchIntroduced is not null)
      {
         var intro = new DatagramPunchIntroduction(peerNodeId.ToArray(), punchToken, peerPublicEp, peerLanEp);
         _ = Events.PunchIntroduced(intro, _cts?.Token ?? CancellationToken.None);
      }
   }

   private void TickActivePunchSessions(long nowTimestamp, List<(byte[] Buffer, int Length, SocketAddress Address)> outgoingBatch)
   {
      foreach (var kvp in _recentPunchTokens)
      {
         if (nowTimestamp >= kvp.Value)
         {
            _recentPunchTokens.TryRemove(kvp.Key, out _);
         }
      }

      if (_activePunches.IsEmpty)
      {
         return;
      }

      Span<byte> probeBody = stackalloc byte[9];

      foreach (var (key, session) in _activePunches)
      {
         if (nowTimestamp >= session.DeadlineTimestamp)
         {
            if (_activePunches.TryRemove(key, out _) && Interlocked.Exchange(ref session.Completed, 1) == 0)
            {
               session.CompletionSource.TrySetResult(new DatagramPunchResult(
                  Succeeded: false,
                  ResolvedEndPoint: null,
                  RoundTripTime: TimeSpan.Zero,
                  NeedsRelayFallback: true));
            }
            continue;
         }

         if (nowTimestamp >= session.NextProbeTimestamp)
         {
            session.NextProbeTimestamp = nowTimestamp + session.ProbeIntervalTicks;
            session.LastProbeSentTimestamp = nowTimestamp;
            DatagramRendezvousCodec.WritePunchProbe(probeBody, session.PunchToken, isReply: false);

            var (pkt1, len1) = BuildUnencryptedControlPacket(DatagramPacketKind.PunchProbe, probeBody);
            outgoingBatch.Add((pkt1, len1, session.PublicAddress));

            if (session.LanAddress is not null)
            {
               var (pkt2, len2) = BuildUnencryptedControlPacket(DatagramPacketKind.PunchProbe, probeBody);
               outgoingBatch.Add((pkt2, len2, session.LanAddress));
            }
         }
      }
   }

   private void SendPunchProbes(ActivePunchSession session, bool isReply)
   {
      Span<byte> probeBody = stackalloc byte[9];
      DatagramRendezvousCodec.WritePunchProbe(probeBody, session.PunchToken, isReply);

      var (pkt1, len1) = BuildUnencryptedControlPacket(DatagramPacketKind.PunchProbe, probeBody);
      DispatchSendAndReturnBuffer(pkt1, len1, session.PublicAddress);

      if (session.LanAddress is not null)
      {
         var (pkt2, len2) = BuildUnencryptedControlPacket(DatagramPacketKind.PunchProbe, probeBody);
         DispatchSendAndReturnBuffer(pkt2, len2, session.LanAddress);
      }
   }

   private (byte[] Buffer, int Length) BuildUnencryptedControlPacket(DatagramPacketKind kind, ReadOnlySpan<byte> body) =>
      BuildStandaloneControlPacket(kind, sessionToken: 0, packetSeq: 0, body, crypto: null);

   private void CancelAllPunchSessions()
   {
      foreach (var kvp in _pendingDiscoveries)
      {
         if (_pendingDiscoveries.TryRemove(kvp.Key, out var d))
         {
            d.CompletionSource.TrySetCanceled();
         }
      }

      foreach (var kvp in _activePunches)
      {
         if (_activePunches.TryRemove(kvp.Key, out var p) && Interlocked.Exchange(ref p.Completed, 1) == 0)
         {
            p.CompletionSource.TrySetCanceled();
         }
      }
   }
}
