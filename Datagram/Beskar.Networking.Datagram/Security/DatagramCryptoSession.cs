using System.Buffers;
using System.Buffers.Binary;
using System.Net;
using System.Security.Cryptography;
using Beskar.Networking.Datagram.Enums;

namespace Beskar.Networking.Datagram.Security;

/// <summary>
/// Stateless server cookie protector and rotating ephemeral ECDH P-256 key manager.
/// </summary>
internal sealed class DatagramServerHandshakeCrypto : IDisposable
{
   private readonly byte[] _cookieKey = new byte[32];
   private readonly DatagramAeadCipher _cipher;
   private readonly TimeProvider _timeProvider;
   private readonly Lock _keyRotationLock = new();

   private uint _currentKeyId = 1;
   private ECDiffieHellman _currentKey;
   private byte[] _currentPubPoint; // 64 bytes: X(32) || Y(32)
   private uint _previousKeyId;
   private ECDiffieHellman? _previousKey;
   private long _lastRotationTimestamp;
   private readonly long _rotationIntervalTicks;

   public DatagramAeadCipher ActiveCipher => _cipher;

   public DatagramServerHandshakeCrypto(DatagramAeadCipher requestedCipher, TimeProvider timeProvider)
   {
      RandomNumberGenerator.Fill(_cookieKey);
      _cipher = DatagramCryptoSession.ResolveCipher(requestedCipher);
      _timeProvider = timeProvider;
      _rotationIntervalTicks = TimeSpan.FromSeconds(60).Ticks * timeProvider.TimestampFrequency / TimeSpan.TicksPerSecond;
      _lastRotationTimestamp = timeProvider.GetTimestamp();

      _currentKey = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
      _currentPubPoint = ExportUncompressedPoint(_currentKey);
   }

   public void GetCurrentEphemeralPublicKey(out uint keyId, Span<byte> publicPoint64)
   {
      MaybeRotateKey();
      lock (_keyRotationLock)
      {
         keyId = _currentKeyId;
         _currentPubPoint.CopyTo(publicPoint64);
      }
   }

   public byte[] IssueCookie(
      IPEndPoint clientEndPoint,
      ReadOnlySpan<byte> clientSalt16,
      ReadOnlySpan<byte> serverSalt16,
      uint serverKeyId,
      long issuedTimestamp)
   {
      // Plaintext: EndPointHash(8) + IssuedTimestamp(8) + ServerKeyId(4) + ClientSalt(16) + ServerSalt(16) = 52 bytes
      Span<byte> plain = stackalloc byte[52];
      BinaryPrimitives.WriteUInt64LittleEndian(plain[0..8], ComputeEndPointHash(clientEndPoint));
      BinaryPrimitives.WriteInt64LittleEndian(plain[8..16], issuedTimestamp);
      BinaryPrimitives.WriteUInt32LittleEndian(plain[16..20], serverKeyId);
      clientSalt16.CopyTo(plain[20..36]);
      serverSalt16.CopyTo(plain[36..52]);

      // Cookie wire: Nonce(12) + Ciphertext(52) + Tag(16) = 80 bytes
      var cookie = new byte[12 + 52 + 16];
      var nonce = cookie.AsSpan(0, 12);
      RandomNumberGenerator.Fill(nonce);

      EncryptWithKey(_cookieKey, _cipher, nonce, plain, cookie.AsSpan(12, 52), cookie.AsSpan(64, 16), ReadOnlySpan<byte>.Empty);
      return cookie;
   }

   public bool TryValidateCookie(
      ReadOnlySpan<byte> cookie,
      IPEndPoint clientEndPoint,
      TimeSpan maxLifetime,
      out ReadOnlySpan<byte> clientSalt,
      out ReadOnlySpan<byte> serverSalt,
      out uint serverKeyId,
      byte[] scratchPlain52)
   {
      clientSalt = default;
      serverSalt = default;
      serverKeyId = 0;

      if (cookie.Length != 80)
      {
         return false;
      }

      var nonce = cookie[0..12];
      var cipherText = cookie[12..64];
      var tag = cookie[64..80];

      if (!TryDecryptWithKey(_cookieKey, _cipher, nonce, cipherText, tag, scratchPlain52.AsSpan(0, 52), ReadOnlySpan<byte>.Empty))
      {
         return false;
      }

      var plain = scratchPlain52.AsSpan(0, 52);
      var expectedHash = ComputeEndPointHash(clientEndPoint);
      var cookieHash = BinaryPrimitives.ReadUInt64LittleEndian(plain[0..8]);
      if (cookieHash != expectedHash)
      {
         return false;
      }

      var issuedTicks = BinaryPrimitives.ReadInt64LittleEndian(plain[8..16]);
      var elapsed = _timeProvider.GetElapsedTime(issuedTicks, _timeProvider.GetTimestamp());
      if (elapsed < TimeSpan.Zero || elapsed > maxLifetime)
      {
         return false;
      }

      serverKeyId = BinaryPrimitives.ReadUInt32LittleEndian(plain[16..20]);
      clientSalt = plain[20..36];
      serverSalt = plain[36..52];
      return true;
   }

   public bool TryDeriveSharedSecret(uint serverKeyId, ReadOnlySpan<byte> clientPubPoint64, out byte[] sharedSecret)
   {
      sharedSecret = [];
      if (clientPubPoint64.Length != 64)
      {
         return false;
      }

      lock (_keyRotationLock)
      {
         ECDiffieHellman? keyToUse;
         if (serverKeyId == _currentKeyId)
         {
            keyToUse = _currentKey;
         }
         else if (_previousKey is not null && serverKeyId == _previousKeyId)
         {
            keyToUse = _previousKey;
         }
         else
         {
            return false;
         }

         try
         {
            using var peerPublic = ImportUncompressedPoint(clientPubPoint64);
            sharedSecret = keyToUse.DeriveRawSecretAgreement(peerPublic.PublicKey);

            return true;
         }
         catch (Exception ex) when (ex is CryptographicException or ArgumentException or ObjectDisposedException)
         {
            return false;
         }
      }
   }

   private void MaybeRotateKey()
   {
      var now = _timeProvider.GetTimestamp();
      if (now - Volatile.Read(ref _lastRotationTimestamp) < _rotationIntervalTicks)
      {
         return;
      }

      lock (_keyRotationLock)
      {
         if (now - _lastRotationTimestamp < _rotationIntervalTicks)
         {
            return;
         }

         _previousKey?.Dispose();
         _previousKey = _currentKey;
         _previousKeyId = _currentKeyId;

         _currentKeyId++;
         _currentKey = ECDiffieHellman.Create(ECCurve.NamedCurves.nistP256);
         _currentPubPoint = ExportUncompressedPoint(_currentKey);
         Volatile.Write(ref _lastRotationTimestamp, now);
      }
   }

   public static byte[] ExportUncompressedPoint(ECDiffieHellman ecdh)
   {
      var p = ecdh.ExportParameters(false);
      var result = new byte[64];

      CopyRightAligned32(p.Q.X!, result.AsSpan(0, 32));
      CopyRightAligned32(p.Q.Y!, result.AsSpan(32, 32));

      return result;
   }

   public static ECDiffieHellman ImportUncompressedPoint(ReadOnlySpan<byte> point64)
   {
      var p = new ECParameters
      {
         Curve = ECCurve.NamedCurves.nistP256,
         Q = new ECPoint
         {
            X = [.. point64[..32]],
            Y = [.. point64[32..64]]
         }
      };
      p.Validate();
      return ECDiffieHellman.Create(p);
   }

   private static void CopyRightAligned32(byte[] source, Span<byte> dest32)
   {
      dest32.Clear();
      var copyLen = Math.Min(source.Length, 32);

      source.AsSpan(source.Length - copyLen, copyLen)
         .CopyTo(dest32[(32 - copyLen)..]);
   }

   private static ulong ComputeEndPointHash(IPEndPoint ep)
   {
      Span<byte> addrBytes = stackalloc byte[18];
      ep.Address.TryWriteBytes(addrBytes, out var len);
      BinaryPrimitives.WriteUInt16LittleEndian(addrBytes.Slice(len, 2), (ushort)ep.Port);

      const ulong offset = 14695981039346656037UL;
      const ulong prime = 1099511628211UL;
      var h = offset;

      for (var i = 0; i < len + 2; i++)
      {
         h ^= addrBytes[i];
         h *= prime;
      }

      return h;
   }

   internal static void EncryptWithKey(
      ReadOnlySpan<byte> key,
      DatagramAeadCipher cipher,
      ReadOnlySpan<byte> nonce,
      ReadOnlySpan<byte> plaintext,
      Span<byte> ciphertext,
      Span<byte> tag,
      ReadOnlySpan<byte> aad)
   {
      if (cipher == DatagramAeadCipher.AesGcm)
      {
         using var aes = new AesGcm(key, 16);
         aes.Encrypt(nonce, plaintext, ciphertext, tag, aad);
      }
      else
      {
         using var chacha = new ChaCha20Poly1305(key);
         chacha.Encrypt(nonce, plaintext, ciphertext, tag, aad);
      }
   }

   internal static bool TryDecryptWithKey(
      ReadOnlySpan<byte> key,
      DatagramAeadCipher cipher,
      ReadOnlySpan<byte> nonce,
      ReadOnlySpan<byte> ciphertext,
      ReadOnlySpan<byte> tag,
      Span<byte> plaintext,
      ReadOnlySpan<byte> aad)
   {
      try
      {
         if (cipher == DatagramAeadCipher.AesGcm)
         {
            using var aes = new AesGcm(key, 16);
            aes.Decrypt(nonce, ciphertext, tag, plaintext, aad);
         }
         else
         {
            using var chacha = new ChaCha20Poly1305(key);
            chacha.Decrypt(nonce, ciphertext, tag, plaintext, aad);
         }

         return true;
      }
      catch (CryptographicException)
      {
         return false;
      }
   }

   public void Dispose()
   {
      lock (_keyRotationLock)
      {
         _currentKey.Dispose();
         _previousKey?.Dispose();
         _previousKey = null;
      }

      CryptographicOperations.ZeroMemory(_cookieKey);
   }
}

/// <summary>
/// Per-peer AEAD encryption/decryption context derived from ECDH + HKDF-SHA256.
/// </summary>
internal sealed class DatagramCryptoSession : IDisposable
{
   public const int TagSize = 16;

   private readonly byte[] _txKey;
   private readonly byte[] _rxKey;
   private readonly byte _txDirectionBit;
   private readonly byte _rxDirectionBit;
   private readonly uint _sessionToken;
   private readonly uint _keyEpoch;

   private readonly AesGcm? _txAes;
   private readonly AesGcm? _rxAes;
   private readonly ChaCha20Poly1305? _txChaCha;
   private readonly ChaCha20Poly1305? _rxChaCha;
   private readonly Lock _txLock = new();
   private readonly Lock _rxLock = new();
   private int _disposed;

   public uint SessionToken => _sessionToken;

   private DatagramCryptoSession(
      DatagramAeadCipher cipher,
      byte[] txKey,
      byte[] rxKey,
      bool isClient,
      uint sessionToken,
      uint keyEpoch = 0)
   {
      _txKey = txKey;
      _rxKey = rxKey;

      _txDirectionBit = isClient ? (byte)0x01 : (byte)0x02;
      _rxDirectionBit = isClient ? (byte)0x02 : (byte)0x01;

      _sessionToken = sessionToken;
      _keyEpoch = keyEpoch;

      if (cipher == DatagramAeadCipher.AesGcm)
      {
         _txAes = new AesGcm(_txKey, TagSize);
         _rxAes = new AesGcm(_rxKey, TagSize);
      }
      else
      {
         _txChaCha = new ChaCha20Poly1305(_txKey);
         _rxChaCha = new ChaCha20Poly1305(_rxKey);
      }
   }

   public static DatagramAeadCipher ResolveCipher(DatagramAeadCipher requested)
   {
      return requested switch
      {
         DatagramAeadCipher.AesGcm when AesGcm.IsSupported => DatagramAeadCipher.AesGcm,
         DatagramAeadCipher.ChaCha20Poly1305 when ChaCha20Poly1305.IsSupported => DatagramAeadCipher.ChaCha20Poly1305,
         DatagramAeadCipher.Auto when AesGcm.IsSupported => DatagramAeadCipher.AesGcm,
         DatagramAeadCipher.Auto when ChaCha20Poly1305.IsSupported => DatagramAeadCipher.ChaCha20Poly1305,
         _ => throw new PlatformNotSupportedException($"Requested AEAD cipher '{requested}' is not supported on this platform.")
      };
   }

   public static DatagramCryptoSession DeriveFromHandshake(
      DatagramAeadCipher requestedCipher,
      ReadOnlySpan<byte> ecdhSharedSecret,
      ReadOnlySpan<byte> clientSalt,
      ReadOnlySpan<byte> serverSalt,
      ReadOnlySpan<byte> preSharedKey,
      bool isClient,
      out uint derivedSessionToken)
   {
      var cipher = ResolveCipher(requestedCipher);

      Span<byte> salt = stackalloc byte[clientSalt.Length + serverSalt.Length];
      clientSalt.CopyTo(salt);
      serverSalt.CopyTo(salt[clientSalt.Length..]);

      var ikmLen = ecdhSharedSecret.Length + preSharedKey.Length;
      var rentedIkm = ikmLen > 512
         ? ArrayPool<byte>.Shared.Rent(ikmLen) : null;
      var ikm = rentedIkm is not null
         ? rentedIkm.AsSpan(0, ikmLen) : stackalloc byte[ikmLen];
      Span<byte> prk = stackalloc byte[32];
      Span<byte> okm = stackalloc byte[68];

      try
      {
         ecdhSharedSecret.CopyTo(ikm);
         if (!preSharedKey.IsEmpty)
         {
            preSharedKey.CopyTo(ikm[ecdhSharedSecret.Length..]);
         }

         // RFC 5869 HKDF-Extract + HKDF-Expand: identical to HKDF.DeriveKey while avoiding
         // Windows CNG's IKM length cap when a large PreSharedKey is supplied.
         HMACSHA256.HashData(salt, ikm, prk);
         HKDF.Expand(HashAlgorithmName.SHA256, prk, okm, "Beskar.Networking.Datagram.v1"u8);

         var c2SKey = okm[..32].ToArray();
         var s2CKey = okm[32..64].ToArray();
         derivedSessionToken = BinaryPrimitives.ReadUInt32LittleEndian(okm[64..68]);

         if (derivedSessionToken == 0)
         {
            derivedSessionToken = 1;
         }

         var txKey = isClient ? c2SKey : s2CKey;
         var rxKey = isClient ? s2CKey : c2SKey;

         return new DatagramCryptoSession(cipher, txKey, rxKey, isClient, derivedSessionToken);
      }
      finally
      {
         CryptographicOperations.ZeroMemory(ikm);
         CryptographicOperations.ZeroMemory(prk);
         CryptographicOperations.ZeroMemory(okm);
         if (rentedIkm is not null)
         {
            ArrayPool<byte>.Shared.Return(rentedIkm);
         }
      }
   }

   public void Encrypt(
      uint packetSeq,
      ReadOnlySpan<byte> aad,
      ReadOnlySpan<byte> plaintext,
      Span<byte> ciphertext,
      Span<byte> tag16)
   {
      Span<byte> nonce = stackalloc byte[12];
      BuildNonce(nonce, _txDirectionBit, _sessionToken, _keyEpoch, packetSeq);

      lock (_txLock)
      {
         ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);

         if (_txAes is not null)
         {
            _txAes.Encrypt(nonce, plaintext, ciphertext, tag16, aad);
         }
         else
         {
            _txChaCha!.Encrypt(nonce, plaintext, ciphertext, tag16, aad);
         }
      }
   }

   public bool TryDecrypt(
      uint packetSeq,
      ReadOnlySpan<byte> aad,
      ReadOnlySpan<byte> ciphertext,
      ReadOnlySpan<byte> tag16,
      Span<byte> plaintext)
   {
      if (Volatile.Read(ref _disposed) == 1)
      {
         return false;
      }

      Span<byte> nonce = stackalloc byte[12];
      BuildNonce(nonce, _rxDirectionBit, _sessionToken, _keyEpoch, packetSeq);

      lock (_rxLock)
      {
         if (Volatile.Read(ref _disposed) == 1)
         {
            return false;
         }

         try
         {
            if (_rxAes is not null)
            {
               _rxAes.Decrypt(nonce, ciphertext, tag16, plaintext, aad);
            }
            else
            {
               _rxChaCha!.Decrypt(nonce, ciphertext, tag16, plaintext, aad);
            }

            return true;
         }
         catch (Exception ex) when (ex is CryptographicException or ObjectDisposedException)
         {
            return false;
         }
      }
   }

   private static void BuildNonce(
      Span<byte> nonce12,
      byte directionBit,
      uint sessionToken,
      uint keyEpoch,
      uint packetSeq)
   {
      nonce12[0] = directionBit;
      BinaryPrimitives.WriteUInt32LittleEndian(nonce12[1..5], sessionToken);

      nonce12[5] = (byte)(keyEpoch & 0xFF);
      nonce12[6] = (byte)((keyEpoch >> 8) & 0xFF);
      nonce12[7] = (byte)((keyEpoch >> 16) & 0xFF);

      BinaryPrimitives.WriteUInt32LittleEndian(nonce12[8..12], packetSeq);
   }

   public void Dispose()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      lock (_txLock)
      {
         _txAes?.Dispose();
         _txChaCha?.Dispose();
         CryptographicOperations.ZeroMemory(_txKey);
      }

      lock (_rxLock)
      {
         _rxAes?.Dispose();
         _rxChaCha?.Dispose();
         CryptographicOperations.ZeroMemory(_rxKey);
      }
   }
}
