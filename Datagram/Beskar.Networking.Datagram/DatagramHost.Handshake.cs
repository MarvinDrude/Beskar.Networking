using System.Buffers;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Net;
using System.Security.Cryptography;
using Beskar.Networking.Datagram.Enums;
using Beskar.Networking.Datagram.Protocol;
using Beskar.Networking.Datagram.Security;
using Beskar.Networking.Datagram.Telemetry;

namespace Beskar.Networking.Datagram;

public sealed partial class DatagramHost
{
   private enum ClientHandshakeStage : byte
   {
      SendingRequest = 0,
      SendingAccept = 1
   }

   private sealed class PendingClientHandshake : IDisposable
   {
      public readonly Lock SyncLock = new();
      public required IPEndPoint RemoteEndPoint { get; init; }
      public required SocketAddress RemoteSocketAddress { get; init; }
      public required byte[] ClientSalt { get; init; }
      public required byte[] ConnectMetadata { get; init; }
      public required TaskCompletionSource<DatagramPeer> CompletionSource { get; init; }

      public ECDiffieHellman? ClientEcdh;
      public byte[]? ClientPub64;
      public byte[]? Cookie80;
      public DatagramCryptoSession? DerivedCrypto;
      public uint DerivedSessionToken;
      public ClientHandshakeStage Stage = ClientHandshakeStage.SendingRequest;
      public long StartedTimestamp;
      public long NextRetryTimestamp;
      public int Completed;

      public void Dispose()
      {
         lock (SyncLock)
         {
            DisposeCryptoLocked();
         }
      }

      public void DisposeCryptoLocked()
      {
         ClientEcdh?.Dispose();
         ClientEcdh = null;
         DerivedCrypto?.Dispose();
         DerivedCrypto = null;
      }
   }

   private sealed record CachedConnectAck(byte[] PacketBytes, long ExpiresAtTimestamp);

   private readonly ConcurrentDictionary<IPEndPoint, PendingClientHandshake> _pendingClientHandshakes = new();
   private readonly ConcurrentDictionary<string, CachedConnectAck> _recentHandshakeAcks = new(StringComparer.Ordinal);
   private readonly ConcurrentDictionary<string, byte> _inFlightServerAccepts = new(StringComparer.Ordinal);

   private int GetMaxConnectMetadataBytesForMtu()
   {
      var mode = _options.Security.Mode;
      var ecPointLen = mode == DatagramSecurityMode.Aead ? DatagramHandshakeCodec.EcPointSize : 0;
      var fixedOverhead = DatagramWireHeader.Size
         + DatagramHandshakeCodec.CookieSize
         + DatagramHandshakeCodec.SaltSize
         + 8 // channelConfigHash
         + 1 // securityMode
         + ecPointLen
         + 2 // metadata length prefix
         + _trailerSize;

      return Math.Max(0, _options.MaxDatagramSize - fixedOverhead);
   }

   /// <summary>
   /// Initiates a 4-step authenticated UDP handshake to <paramref name="remoteEndPoint"/>.
   /// Can be called even on a host that is simultaneously accepting incoming peers.
   /// </summary>
   public async Task<DatagramPeer> ConnectAsync(
      IPEndPoint remoteEndPoint,
      ReadOnlyMemory<byte> connectMetadata = default,
      CancellationToken ct = default)
   {
      if (Volatile.Read(ref _disposed) == 1)
      {
         throw new ObjectDisposedException(nameof(DatagramHost));
      }

      var effectiveMaxMetadata = Math.Min(_options.MaxConnectMetadataBytes, GetMaxConnectMetadataBytesForMtu());
      if (connectMetadata.Length > effectiveMaxMetadata)
      {
         throw new ArgumentOutOfRangeException(nameof(connectMetadata),
            $"ConnectMetadata length ({connectMetadata.Length}) exceeds maximum allowed ({effectiveMaxMetadata} bytes for MaxDatagramSize={_options.MaxDatagramSize}).");
      }

      if (Volatile.Read(ref _started) == 0)
      {
         Start();
      }

      remoteEndPoint = NormalizeTargetEndPoint(remoteEndPoint);

      var clientSalt = new byte[DatagramHandshakeCodec.SaltSize];
      RandomNumberGenerator.Fill(clientSalt);

      ECDiffieHellman? ecdh = null;
      byte[]? pub64 = null;
      if (_options.Security.Mode == DatagramSecurityMode.Aead)
      {
         ecdh = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
         pub64 = DatagramServerHandshakeCrypto.ExportUncompressedPoint(ecdh);
      }

      var now = _timeProvider.GetTimestamp();
      var retryIntervalTicks = _options.HandshakeRetryInterval.Ticks * _timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond;

      var pending = new PendingClientHandshake
      {
         RemoteEndPoint = remoteEndPoint,
         RemoteSocketAddress = remoteEndPoint.Serialize(),
         ClientSalt = clientSalt,
         ConnectMetadata = connectMetadata.IsEmpty ? [] : connectMetadata.ToArray(),
         CompletionSource = new TaskCompletionSource<DatagramPeer>(TaskCreationOptions.RunContinuationsAsynchronously),
         ClientEcdh = ecdh,
         ClientPub64 = pub64,
         StartedTimestamp = now,
         NextRetryTimestamp = now + retryIntervalTicks
      };

      if (!_pendingClientHandshakes.TryAdd(remoteEndPoint, pending))
      {
         pending.Dispose();
         throw new InvalidOperationException($"A handshake to {remoteEndPoint} is already in progress.");
      }

      using var reg = ct.Register(() =>
      {
         if (_pendingClientHandshakes.TryRemove(remoteEndPoint, out var removed))
         {
            var shouldCancel = false;
            lock (removed.SyncLock)
            {
               if (Interlocked.Exchange(ref removed.Completed, 1) == 0)
               {
                  removed.DisposeCryptoLocked();
                  shouldCancel = true;
               }
            }

            if (shouldCancel)
            {
               removed.CompletionSource.TrySetCanceled(ct);
            }
         }
      });

      var (reqBuf, reqLen) = BuildConnectRequestPacket(pending);
      DispatchSendAndReturnBuffer(reqBuf, reqLen, pending.RemoteSocketAddress);

      return await pending.CompletionSource.Task;
   }

   private (byte[] Buffer, int Length) BuildConnectRequestPacket(PendingClientHandshake pending)
   {
      var useCrc = _options.Security.Mode != DatagramSecurityMode.None;
      var crcLen = useCrc ? Crc32C.Size : 0;
      var targetBodyLen = _options.Security.PadConnectRequests
         ? _options.MaxDatagramSize - DatagramWireHeader.Size - crcLen
         : 0;

      var buf = ArrayPool<byte>.Shared.Rent(_options.MaxDatagramSize + 16);
      var header = new DatagramWireHeader(DatagramPacketKind.ConnectRequest, DatagramWireHeader.CurrentVersion, false, 0, 0, 0, 0);
      header.Write(buf.AsSpan(0, DatagramWireHeader.Size));

      var bodyLen = DatagramHandshakeCodec.WriteConnectRequest(
         buf.AsSpan(DatagramWireHeader.Size),
         pending.ClientSalt,
         _channelConfigHash,
         _options.Security.Mode,
         pending.ClientPub64 ?? ReadOnlySpan<byte>.Empty,
         targetBodyLen);

      var totalWithoutTrailer = DatagramWireHeader.Size + bodyLen;
      if (useCrc)
      {
         var crc = Crc32C.Compute(buf.AsSpan(0, totalWithoutTrailer));
         BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(totalWithoutTrailer, Crc32C.Size), crc);
         return (buf, totalWithoutTrailer + Crc32C.Size);
      }

      return (buf, totalWithoutTrailer);
   }

   private (byte[] Buffer, int Length) BuildConnectAcceptPacket(PendingClientHandshake pending)
   {
      var mode = _options.Security.Mode;
      var isAead = mode == DatagramSecurityMode.Aead;
      var trailerLen = mode switch
      {
         DatagramSecurityMode.Aead => DatagramCryptoSession.TagSize,
         DatagramSecurityMode.Crc32C => Crc32C.Size,
         _ => 0
      };

      var requiredSize = DatagramWireHeader.Size
         + DatagramHandshakeCodec.CookieSize
         + DatagramHandshakeCodec.SaltSize
         + 8
         + 1
         + (isAead ? DatagramHandshakeCodec.EcPointSize : 0)
         + 2
         + pending.ConnectMetadata.Length
         + trailerLen;

      var buf = ArrayPool<byte>.Shared.Rent(Math.Max(_options.MaxDatagramSize + 64, requiredSize));
      var header = new DatagramWireHeader(
         DatagramPacketKind.ConnectAccept,
         DatagramWireHeader.CurrentVersion,
         isAead,
         pending.DerivedSessionToken,
         PacketSeq: 1,
         AckSeq: 0,
         AckBitfield: 0);

      header.Write(buf.AsSpan(0, DatagramWireHeader.Size));

      var prefixLen = DatagramHandshakeCodec.WriteConnectAcceptPrefix(
         buf.AsSpan(DatagramWireHeader.Size),
         pending.Cookie80!,
         pending.ClientSalt,
         _channelConfigHash,
         mode,
         pending.ClientPub64 ?? ReadOnlySpan<byte>.Empty);

      var tailOffset = DatagramWireHeader.Size + prefixLen;
      var tailLen = DatagramHandshakeCodec.WriteConnectAcceptTail(buf.AsSpan(tailOffset), pending.ConnectMetadata);

      if (isAead)
      {
         byte[]? rentedPlain = tailLen > 1024 ? ArrayPool<byte>.Shared.Rent(tailLen) : null;
         Span<byte> plainTail = rentedPlain is not null ? rentedPlain.AsSpan(0, tailLen) : stackalloc byte[tailLen];

         try
         {
            buf.AsSpan(tailOffset, tailLen).CopyTo(plainTail);
            pending.DerivedCrypto!.Encrypt(
               packetSeq: 1,
               aad: buf.AsSpan(0, tailOffset),
               plaintext: plainTail,
               ciphertext: buf.AsSpan(tailOffset, tailLen),
               tag16: buf.AsSpan(tailOffset + tailLen, DatagramCryptoSession.TagSize));
         }
         finally
         {
            if (rentedPlain is not null)
            {
               ArrayPool<byte>.Shared.Return(rentedPlain);
            }
         }
      }
      else if (mode == DatagramSecurityMode.Crc32C)
      {
         var crc = Crc32C.Compute(buf.AsSpan(0, tailOffset + tailLen));
         BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(tailOffset + tailLen, Crc32C.Size), crc);
      }

      return (buf, tailOffset + tailLen + trailerLen);
   }

   private void HandleConnectRequest(ReadOnlySpan<byte> rawPacket, SocketAddress remoteSa)
   {
      if (!TryVerifyHandshakeCrc(rawPacket, out var body))
      {
         DatagramMetrics.SecurityRejections.Add(1);
         return;
      }

      if (!DatagramHandshakeCodec.TryReadConnectRequest(
             body,
             out var clientSalt16,
             out var configHash,
             out var reqMode,
             out _))
      {
         return;
      }

      if (reqMode != _options.Security.Mode)
      {
         return;
      }

      var senderEp = MaterializeEndPoint(remoteSa);
      Span<byte> serverSalt16 = stackalloc byte[DatagramHandshakeCodec.SaltSize];
      RandomNumberGenerator.Fill(serverSalt16);

      uint serverKeyId = 0;
      Span<byte> serverPub64 = stackalloc byte[DatagramHandshakeCodec.EcPointSize];
      if (_options.Security.Mode == DatagramSecurityMode.Aead)
      {
         _serverHandshakeCrypto!.GetCurrentEphemeralPublicKey(out serverKeyId, serverPub64);
      }
      else
      {
         serverPub64.Clear();
      }

      var cookie80 = _serverHandshakeCrypto!.IssueCookie(
         senderEp,
         clientSalt16,
         serverSalt16,
         serverKeyId,
         _timeProvider.GetTimestamp());

      var useCrc = _options.Security.Mode != DatagramSecurityMode.None;
      var buf = ArrayPool<byte>.Shared.Rent(512);
      var header = new DatagramWireHeader(DatagramPacketKind.ConnectChallenge, DatagramWireHeader.CurrentVersion, false, 0, 0, 0, 0);
      header.Write(buf.AsSpan(0, DatagramWireHeader.Size));

      var challengeBodyLen = DatagramHandshakeCodec.WriteConnectChallenge(
         buf.AsSpan(DatagramWireHeader.Size),
         cookie80,
         serverSalt16,
         _options.Security.Mode,
         serverKeyId,
         serverPub64);

      var totalLen = DatagramWireHeader.Size + challengeBodyLen;
      if (useCrc)
      {
         var crc = Crc32C.Compute(buf.AsSpan(0, totalLen));
         BinaryPrimitives.WriteUInt32LittleEndian(buf.AsSpan(totalLen, Crc32C.Size), crc);
         totalLen += Crc32C.Size;
      }

      DispatchSendAndReturnBuffer(buf, totalLen, senderEp.Serialize());
   }

   private void HandleConnectChallenge(ReadOnlySpan<byte> rawPacket, SocketAddress remoteSa)
   {
      if (!TryVerifyHandshakeCrc(rawPacket, out var body))
      {
         DatagramMetrics.SecurityRejections.Add(1);
         return;
      }

      var senderEp = MaterializeEndPoint(remoteSa);
      if (!_pendingClientHandshakes.TryGetValue(senderEp, out var pending))
      {
         return;
      }

      if (pending.Stage != ClientHandshakeStage.SendingRequest)
      {
         return;
      }

      if (!DatagramHandshakeCodec.TryReadConnectChallenge(
             body,
             out var cookie80,
             out var serverSalt16,
             out var challengeMode,
             out _,
             out var serverPub64))
      {
         return;
      }

      if (challengeMode != _options.Security.Mode)
      {
         return;
      }

      byte[]? acceptBuf;
      int acceptLen;
      SocketAddress targetAddr;

      lock (pending.SyncLock)
      {
         if (Volatile.Read(ref pending.Completed) != 0 || pending.Stage != ClientHandshakeStage.SendingRequest)
         {
            return;
         }

         if (challengeMode == DatagramSecurityMode.Aead)
         {
            if (pending.ClientEcdh is null)
            {
               return;
            }

            try
            {
               using var serverPub = DatagramServerHandshakeCrypto.ImportUncompressedPoint(serverPub64);
               var sharedSecret = pending.ClientEcdh.DeriveRawSecretAgreement(serverPub.PublicKey);
               var psk = _options.Security.PreSharedKey ?? ReadOnlySpan<byte>.Empty;

               pending.DerivedCrypto = DatagramCryptoSession.DeriveFromHandshake(
                  _options.Security.AeadCipher,
                  sharedSecret,
                  pending.ClientSalt,
                  serverSalt16,
                  psk,
                  isClient: true,
                  out var derivedToken);

               CryptographicOperations.ZeroMemory(sharedSecret);
               pending.DerivedSessionToken = derivedToken;
            }
            catch (Exception ex) when (ex is CryptographicException or ArgumentException or ObjectDisposedException)
            {
               DatagramMetrics.SecurityRejections.Add(1);
               return;
            }
         }

         pending.Cookie80 = cookie80.ToArray();
         pending.Stage = ClientHandshakeStage.SendingAccept;
         var retryIntervalTicks = _options.HandshakeRetryInterval.Ticks * _timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond;
         pending.NextRetryTimestamp = _timeProvider.GetTimestamp() + retryIntervalTicks;

         (acceptBuf, acceptLen) = BuildConnectAcceptPacket(pending);
         targetAddr = pending.RemoteSocketAddress;
      }

      DispatchSendAndReturnBuffer(acceptBuf, acceptLen, targetAddr);
   }

   private ValueTask HandleConnectAcceptAsync(ReadOnlyMemory<byte> rawMemory, SocketAddress remoteSa, CancellationToken ct)
   {
      var rawSpan = rawMemory.Span;
      if (rawSpan.Length < DatagramWireHeader.Size
          || !DatagramWireHeader.TryRead(rawSpan, out var header))
      {
         return ValueTask.CompletedTask;
      }

      var mode = _options.Security.Mode;
      var trailerLen = mode switch
      {
         DatagramSecurityMode.Aead => DatagramCryptoSession.TagSize,
         DatagramSecurityMode.Crc32C => Crc32C.Size,
         _ => 0
      };

      if (rawSpan.Length < DatagramWireHeader.Size + trailerLen)
      {
         return ValueTask.CompletedTask;
      }

      var bodyAndPrefix = rawSpan.Slice(DatagramWireHeader.Size, rawSpan.Length - DatagramWireHeader.Size - trailerLen);
      if (!DatagramHandshakeCodec.TryReadConnectAcceptPrefix(
             bodyAndPrefix,
             out var cookie80,
             out var clientSalt16,
             out var configHash,
             out var reqMode,
             out var clientPub64,
             out var prefixLen))
      {
         return ValueTask.CompletedTask;
      }

      if (reqMode != mode
          || header.IsEncrypted != (mode == DatagramSecurityMode.Aead)
          || header.PacketSeq != 1)
      {
         DatagramMetrics.SecurityRejections.Add(1);
         return ValueTask.CompletedTask;
      }

      var senderEp = MaterializeEndPoint(remoteSa);
      var saltKey = $"{senderEp}|{Convert.ToHexString(clientSalt16)}";

      if (_recentHandshakeAcks.TryGetValue(saltKey, out var cachedAck))
      {
         var dupCopy = ArrayPool<byte>.Shared.Rent(cachedAck.PacketBytes.Length);
         cachedAck.PacketBytes.CopyTo(dupCopy.AsSpan());
         DispatchSendAndReturnBuffer(dupCopy, cachedAck.PacketBytes.Length, senderEp.Serialize());
         return ValueTask.CompletedTask;
      }

      if (!_inFlightServerAccepts.TryAdd(saltKey, 0))
      {
         return ValueTask.CompletedTask; // Duplicate retry while ValidateConnection is already running
      }

      var handedOffToAsync = false;
      try
      {
         var scratchArr = new byte[52];
         if (!_serverHandshakeCrypto!.TryValidateCookie(
                cookie80,
                senderEp,
                _options.Security.ChallengeCookieLifetime,
                out var cookieClientSalt,
                out var cookieServerSalt,
                out var serverKeyId,
                scratchArr)
             || !cookieClientSalt.SequenceEqual(clientSalt16))
         {
            DatagramMetrics.SecurityRejections.Add(1);
            return ValueTask.CompletedTask;
         }

         var tailOffset = DatagramWireHeader.Size + prefixLen;
         var tailLen = bodyAndPrefix.Length - prefixLen;

         if (tailLen < 2)
         {
            return ValueTask.CompletedTask;
         }

         DatagramCryptoSession? crypto = null;
         uint sessionToken;
         var plainTailCopy = new byte[tailLen];

         if (mode == DatagramSecurityMode.Aead)
         {
            if (!_serverHandshakeCrypto.TryDeriveSharedSecret(serverKeyId, clientPub64, out var sharedSecret))
            {
               DatagramMetrics.SecurityRejections.Add(1);
               return ValueTask.CompletedTask;
            }

            var psk = _options.Security.PreSharedKey ?? ReadOnlySpan<byte>.Empty;
            crypto = DatagramCryptoSession.DeriveFromHandshake(
               _options.Security.AeadCipher,
               sharedSecret,
               clientSalt16,
               cookieServerSalt,
               psk,
               isClient: false,
               out sessionToken);
            CryptographicOperations.ZeroMemory(sharedSecret);

            var aad = rawSpan[0..tailOffset];
            var cipherTail = rawSpan.Slice(tailOffset, tailLen);
            var tag = rawSpan.Slice(tailOffset + tailLen, DatagramCryptoSession.TagSize);

            if (!crypto.TryDecrypt(header.PacketSeq, aad, cipherTail, tag, plainTailCopy))
            {
               crypto.Dispose();
               DatagramMetrics.SecurityRejections.Add(1);
               return ValueTask.CompletedTask;
            }
         }
         else
         {
            if (mode == DatagramSecurityMode.Crc32C)
            {
               var computed = Crc32C.Compute(rawSpan[0..(tailOffset + tailLen)]);
               var wireCrc = BinaryPrimitives.ReadUInt32LittleEndian(rawSpan.Slice(tailOffset + tailLen, Crc32C.Size));
               if (computed != wireCrc)
               {
                  DatagramMetrics.SecurityRejections.Add(1);
                  return ValueTask.CompletedTask;
               }
            }

            rawSpan.Slice(tailOffset, tailLen).CopyTo(plainTailCopy);
            sessionToken = AllocateUniqueSessionToken();
         }

         if (!DatagramHandshakeCodec.TryReadConnectAcceptTail(plainTailCopy, out var metadataSpan)
             || metadataSpan.Length > _options.MaxConnectMetadataBytes)
         {
            crypto?.Dispose();
            return ValueTask.CompletedTask;
         }

         var metadataBytes = metadataSpan.ToArray();
         var packetSeq = header.PacketSeq;

         handedOffToAsync = true;
         return FinishConnectAcceptAsync(senderEp, saltKey, configHash, metadataBytes, crypto, sessionToken, packetSeq);
      }
      finally
      {
         if (!handedOffToAsync)
         {
            _inFlightServerAccepts.TryRemove(saltKey, out _);
         }
      }
   }

   private async ValueTask FinishConnectAcceptAsync(
      IPEndPoint senderEp,
      string saltKey,
      ulong configHash,
      byte[] metadataBytes,
      DatagramCryptoSession? crypto,
      uint sessionToken,
      uint remotePacketSeq)
   {
      var cryptoTransferred = false;

      try
      {
         var accepted = true;
         byte[]? rejectReason = null;
         object? initialTag = null;

         if (configHash != _channelConfigHash)
         {
            accepted = false;
            rejectReason = "Channel configuration mismatch."u8.ToArray();
         }
         else if (_peersById.Count >= _options.MaxPeers)
         {
            accepted = false;
            rejectReason = "Host peer limit reached."u8.ToArray();
         }
         else if (Events.ValidateConnection is not null)
         {
            var ctx = new DatagramConnectionRequestContext
            {
               RemoteEndPoint = senderEp,
               ConnectMetadata = metadataBytes,
               ChannelConfigHash = configHash
            };

            await Events.ValidateConnection(ctx);
            accepted = ctx.Accepted;
            rejectReason = ctx.RejectReason;
            initialTag = ctx.PeerTag;
         }

         if (Volatile.Read(ref _disposed) == 1)
         {
            return;
         }

         var now = _timeProvider.GetTimestamp();

         if (!accepted)
         {
            var maxRejectBytes = Math.Max(0, Math.Min(2048 - 11, _options.MaxDatagramSize - DatagramWireHeader.Size - _trailerSize - 11));
            var reasonSpan = rejectReason is null
               ? ReadOnlySpan<byte>.Empty
               : rejectReason.AsSpan(0, Math.Min(rejectReason.Length, maxRejectBytes));

            var rejLen = 11 + reasonSpan.Length;
            var rentedRejBody = rejLen > 512 ? ArrayPool<byte>.Shared.Rent(rejLen) : null;
            var rejBody = rentedRejBody is not null ? rentedRejBody.AsSpan(0, rejLen) : stackalloc byte[rejLen];

            try
            {
               DatagramHandshakeCodec.WriteConnectAck(rejBody, accepted: false, 0, 0, reasonSpan);

               var (rejPkt, rejPktLen) = BuildStandaloneControlPacket(DatagramPacketKind.ConnectAck, sessionToken, 1, rejBody, crypto);
               DispatchSendAndReturnBuffer(rejPkt, rejPktLen, senderEp.Serialize());
            }
            finally
            {
               if (rentedRejBody is not null)
               {
                  ArrayPool<byte>.Shared.Return(rentedRejBody);
               }
            }
            return;
         }

         if (_peersBySessionToken.ContainsKey(sessionToken))
         {
            return;
         }

         var peerId = Interlocked.Increment(ref _nextPeerId);
         var peer = new DatagramPeer(
            peerId,
            sessionToken,
            isOutboundInitiator: false,
            senderEp,
            metadataBytes,
            _options,
            _channelTable,
            _priorityChannels,
            crypto,
            _timeProvider,
            initialTag);

         cryptoTransferred = true;
         peer.InitializeHandshakeSeqs(lastRemotePacketSeq: remotePacketSeq, nextLocalPacketSeq: 1);

         if (Volatile.Read(ref _disposed) == 1
             || !_peersBySessionToken.TryAdd(sessionToken, peer)
             || !_peersById.TryAdd(peerId, peer))
         {
            _peersBySessionToken.TryRemove(KeyValuePair.Create(sessionToken, peer));
            _peersById.TryRemove(KeyValuePair.Create(peerId, peer));
            peer.Dispose();
            return;
         }

         Span<byte> ackBody = stackalloc byte[11];
         DatagramHandshakeCodec.WriteConnectAck(ackBody, accepted: true, peerId, sessionToken, ReadOnlySpan<byte>.Empty);
         var ackPacket = peer.BuildRawPacketSnapshot(DatagramPacketKind.ConnectAck, ackBody, now, out var ackLen);

         var ackCopy = ackPacket.AsSpan(0, ackLen).ToArray();
         var cacheExpiry = now + (_options.Security.ChallengeCookieLifetime.Ticks * _timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond);
         _recentHandshakeAcks[saltKey] = new CachedConnectAck(ackCopy, cacheExpiry);

         DispatchSendAndReturnBuffer(ackPacket, ackLen, senderEp.Serialize());

         DatagramMetrics.PeersActive.Add(1);
         DatagramMetrics.PeersConnected.Add(1);
         EmitEvent(DatagramEvent.PeerConnected(peer));
      }
      finally
      {
         _inFlightServerAccepts.TryRemove(saltKey, out _);
         if (!cryptoTransferred)
         {
            crypto?.Dispose();
         }
      }
   }

   private void HandleConnectAck(in DatagramWireHeader header, ReadOnlySpan<byte> rawPacket, SocketAddress remoteSa)
   {
      var senderEp = MaterializeEndPoint(remoteSa);
      if (!_pendingClientHandshakes.TryGetValue(senderEp, out var pending))
      {
         return;
      }

      var mode = _options.Security.Mode;
      var trailerLen = mode switch
      {
         DatagramSecurityMode.Aead => DatagramCryptoSession.TagSize,
         DatagramSecurityMode.Crc32C => Crc32C.Size,
         _ => 0
      };

      if (rawPacket.Length < DatagramWireHeader.Size + trailerLen || header.IsEncrypted != (mode == DatagramSecurityMode.Aead))
      {
         DatagramMetrics.SecurityRejections.Add(1);
         return;
      }

      var bodyLen = rawPacket.Length - DatagramWireHeader.Size - trailerLen;
      if (bodyLen > 2048)
      {
         DatagramMetrics.SecurityRejections.Add(1);
         return;
      }

      bool accepted;
      uint assignedSessionToken;
      byte[]? rejectReasonBytes = null;
      DatagramCryptoSession? crypto = null;
      byte[] connectMetadata;

      lock (pending.SyncLock)
      {
         if (Volatile.Read(ref pending.Completed) != 0 || pending.Stage != ClientHandshakeStage.SendingAccept)
         {
            return;
         }

         Span<byte> plainBody = stackalloc byte[bodyLen];

         if (mode == DatagramSecurityMode.Aead)
         {
            if (pending.DerivedCrypto is null || !pending.DerivedCrypto.TryDecrypt(
                   header.PacketSeq,
                   rawPacket[0..DatagramWireHeader.Size],
                   rawPacket.Slice(DatagramWireHeader.Size, bodyLen),
                   rawPacket.Slice(DatagramWireHeader.Size + bodyLen, DatagramCryptoSession.TagSize),
                   plainBody))
            {
               DatagramMetrics.SecurityRejections.Add(1);
               return;
            }
         }
         else
         {
            if (mode == DatagramSecurityMode.Crc32C)
            {
               var computed = Crc32C.Compute(rawPacket[0..(DatagramWireHeader.Size + bodyLen)]);
               var wireCrc = BinaryPrimitives.ReadUInt32LittleEndian(rawPacket.Slice(DatagramWireHeader.Size + bodyLen, Crc32C.Size));
               if (computed != wireCrc)
               {
                  DatagramMetrics.SecurityRejections.Add(1);
                  return;
               }
            }
            rawPacket.Slice(DatagramWireHeader.Size, bodyLen).CopyTo(plainBody);
         }

         if (!DatagramHandshakeCodec.TryReadConnectAck(
                plainBody,
                out accepted,
                out _,
                out assignedSessionToken,
                out var rejectReason))
         {
            return;
         }

         if (accepted && (assignedSessionToken == 0 || (mode == DatagramSecurityMode.Aead && assignedSessionToken != pending.DerivedSessionToken)))
         {
            DatagramMetrics.SecurityRejections.Add(1);
            return;
         }

         if (!_pendingClientHandshakes.TryRemove(senderEp, out _)
             || Interlocked.Exchange(ref pending.Completed, 1) == 1)
         {
            return;
         }

         pending.ClientEcdh?.Dispose();
         pending.ClientEcdh = null;
         connectMetadata = pending.ConnectMetadata;

         if (!accepted)
         {
            rejectReasonBytes = rejectReason.IsEmpty ? [] : rejectReason.ToArray();
            pending.DerivedCrypto?.Dispose();
            pending.DerivedCrypto = null;
         }
         else
         {
            crypto = pending.DerivedCrypto;
            pending.DerivedCrypto = null; // Transferred to peer
         }
      }

      if (!accepted)
      {
         var msg = rejectReasonBytes!.Length == 0
            ? "Connection rejected by remote host."
            : System.Text.Encoding.UTF8.GetString(rejectReasonBytes);

         pending.CompletionSource.TrySetException(new DatagramConnectionRejectedException(msg, rejectReasonBytes));
         return;
      }

      if (Volatile.Read(ref _disposed) == 1)
      {
         crypto?.Dispose();
         pending.CompletionSource.TrySetCanceled();
         return;
      }

      var peerId = Interlocked.Increment(ref _nextPeerId);

      var peer = new DatagramPeer(
         peerId,
         assignedSessionToken,
         isOutboundInitiator: true,
         senderEp,
         connectMetadata,
         _options,
         _channelTable,
         _priorityChannels,
         crypto,
         _timeProvider);

      peer.InitializeHandshakeSeqs(lastRemotePacketSeq: header.PacketSeq, nextLocalPacketSeq: 2);

      if (Volatile.Read(ref _disposed) == 1)
      {
         peer.Dispose();
         pending.CompletionSource.TrySetCanceled();
         return;
      }

      if (!_peersBySessionToken.TryAdd(assignedSessionToken, peer)
          || !_peersById.TryAdd(peerId, peer))
      {
         _peersBySessionToken.TryRemove(KeyValuePair.Create(assignedSessionToken, peer));
         _peersById.TryRemove(KeyValuePair.Create(peerId, peer));

         peer.Dispose();
         pending.CompletionSource.TrySetException(
            new InvalidOperationException($"Session token collision ({assignedSessionToken}) while registering peer."));

         return;
      }

      DatagramMetrics.PeersActive.Add(1);
      DatagramMetrics.PeersConnected.Add(1);

      pending.CompletionSource.TrySetResult(peer);
      EmitEvent(DatagramEvent.PeerConnected(peer));
   }

   private void TickPendingHandshakes(long nowTimestamp, List<(byte[] Buffer, int Length, SocketAddress Address)> outgoingBatch)
   {
      if (_pendingClientHandshakes.IsEmpty)
      {
         return;
      }

      var timeoutTicks = _options.ConnectTimeout.Ticks * _timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond;
      var retryIntervalTicks = _options.HandshakeRetryInterval.Ticks * _timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond;

      foreach (var (key, pending) in _pendingClientHandshakes)
      {
         var timedOut = false;
         (byte[] Buffer, int Length, SocketAddress Address)? retryPacket = null;

         lock (pending.SyncLock)
         {
            if (Volatile.Read(ref pending.Completed) != 0)
            {
               continue;
            }

            if (nowTimestamp - pending.StartedTimestamp >= timeoutTicks)
            {
               if (_pendingClientHandshakes.TryRemove(key, out _) && Interlocked.Exchange(ref pending.Completed, 1) == 0)
               {
                  pending.DisposeCryptoLocked();
                  timedOut = true;
               }
            }
            else if (nowTimestamp >= pending.NextRetryTimestamp)
            {
               pending.NextRetryTimestamp = nowTimestamp + retryIntervalTicks;
               var (buf, len) = pending.Stage == ClientHandshakeStage.SendingRequest
                  ? BuildConnectRequestPacket(pending)
                  : BuildConnectAcceptPacket(pending);
               retryPacket = (buf, len, pending.RemoteSocketAddress);
            }
         }

         if (timedOut)
         {
            pending.CompletionSource.TrySetException(new TimeoutException($"Datagram handshake to {key} timed out."));
         }
         else if (retryPacket.HasValue)
         {
            outgoingBatch.Add(retryPacket.Value);
         }
      }
   }

   private void CleanupExpiredAckCache(long nowTimestamp)
   {
      foreach (var kvp in _recentHandshakeAcks)
      {
         if (nowTimestamp >= kvp.Value.ExpiresAtTimestamp)
         {
            _recentHandshakeAcks.TryRemove(kvp.Key, out _);
         }
      }
   }

   private void CancelAllPendingHandshakes()
   {
      foreach (var kvp in _pendingClientHandshakes)
      {
         if (_pendingClientHandshakes.TryRemove(kvp.Key, out var pending))
         {
            var shouldCancel = false;
            lock (pending.SyncLock)
            {
               if (Interlocked.Exchange(ref pending.Completed, 1) == 0)
               {
                  pending.DisposeCryptoLocked();
                  shouldCancel = true;
               }
            }

            if (shouldCancel)
            {
               pending.CompletionSource.TrySetCanceled();
            }
         }
      }
   }

   private bool TryVerifyHandshakeCrc(ReadOnlySpan<byte> rawPacket, out ReadOnlySpan<byte> body)
   {
      body = default;
      if (_options.Security.Mode != DatagramSecurityMode.None)
      {
         if (rawPacket.Length < DatagramWireHeader.Size + Crc32C.Size) return false;
         var end = rawPacket.Length - Crc32C.Size;
         var computed = Crc32C.Compute(rawPacket[0..end]);
         var wire = BinaryPrimitives.ReadUInt32LittleEndian(rawPacket.Slice(end, Crc32C.Size));

         if (computed != wire) return false;
         body = rawPacket[DatagramWireHeader.Size..end];
      }
      else
      {
         body = rawPacket[DatagramWireHeader.Size..];
      }

      return true;
   }

   private (byte[] Buffer, int Length) BuildStandaloneControlPacket(
      DatagramPacketKind kind,
      uint sessionToken,
      uint packetSeq,
      ReadOnlySpan<byte> body,
      DatagramCryptoSession? crypto)
   {
      var mode = _options.Security.Mode;
      var isAead = mode == DatagramSecurityMode.Aead && crypto is not null;
      var useCrc = !isAead && mode != DatagramSecurityMode.None;
      var trailerLen = isAead ? DatagramCryptoSession.TagSize : (useCrc ? Crc32C.Size : 0);
      var totalLen = DatagramWireHeader.Size + body.Length + trailerLen;
      var buf = ArrayPool<byte>.Shared.Rent(totalLen);
      var span = buf.AsSpan(0, totalLen);

      var header = new DatagramWireHeader(kind, DatagramWireHeader.CurrentVersion, isAead, sessionToken, packetSeq, 0, 0);
      header.Write(span[0..DatagramWireHeader.Size]);

      if (isAead)
      {
         crypto!.Encrypt(
            packetSeq,
            span[0..DatagramWireHeader.Size],
            body,
            span.Slice(DatagramWireHeader.Size, body.Length),
            span.Slice(DatagramWireHeader.Size + body.Length, DatagramCryptoSession.TagSize));
      }
      else
      {
         body.CopyTo(span.Slice(DatagramWireHeader.Size, body.Length));
         if (useCrc)
         {
            var crc = Crc32C.Compute(span[0..(DatagramWireHeader.Size + body.Length)]);
            BinaryPrimitives.WriteUInt32LittleEndian(span.Slice(DatagramWireHeader.Size + body.Length, Crc32C.Size), crc);
         }
      }

      return (buf, totalLen);
   }
}
