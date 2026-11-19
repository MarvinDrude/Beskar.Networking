using System.Buffers.Binary;
using Beskar.Networking.Datagram.Enums;

namespace Beskar.Networking.Datagram.Protocol;

/// <summary>
/// Encodes and decodes 4-step handshake frames (<c>ConnectRequest</c>, <c>ConnectChallenge</c>,
/// <c>ConnectAccept</c>, and <c>ConnectAck</c>).
/// </summary>
internal static class DatagramHandshakeCodec
{
   public const int SaltSize = 16;
   public const int CookieSize = 80;
   public const int EcPointSize = 64;

   public static int WriteConnectRequest(
      Span<byte> destinationBody,
      ReadOnlySpan<byte> clientSalt16,
      ulong channelConfigHash,
      DatagramSecurityMode securityMode,
      ReadOnlySpan<byte> clientEcdhPub64,
      int targetPaddedBodyLength)
   {
      var offset = 0;
      clientSalt16.CopyTo(destinationBody.Slice(offset, SaltSize));
      offset += SaltSize;

      BinaryPrimitives.WriteUInt64LittleEndian(destinationBody.Slice(offset, 8), channelConfigHash);
      offset += 8;

      destinationBody[offset++] = (byte)securityMode;
      if (securityMode == DatagramSecurityMode.Aead)
      {
         clientEcdhPub64.CopyTo(destinationBody.Slice(offset, EcPointSize));
         offset += EcPointSize;
      }

      if (targetPaddedBodyLength > offset)
      {
         destinationBody[offset..targetPaddedBodyLength].Clear();
         offset = targetPaddedBodyLength;
      }

      return offset;
   }

   public static bool TryReadConnectRequest(
      ReadOnlySpan<byte> body,
      out ReadOnlySpan<byte> clientSalt16,
      out ulong channelConfigHash,
      out DatagramSecurityMode securityMode,
      out ReadOnlySpan<byte> clientEcdhPub64)
   {
      clientSalt16 = default;
      channelConfigHash = 0;
      securityMode = DatagramSecurityMode.None;
      clientEcdhPub64 = default;

      if (body.Length < SaltSize + 8 + 1)
      {
         return false;
      }

      clientSalt16 = body[0..SaltSize];
      channelConfigHash = BinaryPrimitives.ReadUInt64LittleEndian(body[16..24]);
      securityMode = (DatagramSecurityMode)body[24];

      if (!Enum.IsDefined(securityMode))
      {
         return false;
      }

      if (securityMode == DatagramSecurityMode.Aead)
      {
         if (body.Length < 25 + EcPointSize) return false;
         clientEcdhPub64 = body.Slice(25, EcPointSize);
      }

      return true;
   }

   public static int WriteConnectChallenge(
      Span<byte> destinationBody,
      ReadOnlySpan<byte> cookie80,
      ReadOnlySpan<byte> serverSalt16,
      DatagramSecurityMode securityMode,
      uint serverKeyId,
      ReadOnlySpan<byte> serverEcdhPub64)
   {
      var offset = 0;
      cookie80.CopyTo(destinationBody.Slice(offset, CookieSize));
      offset += CookieSize;

      serverSalt16.CopyTo(destinationBody.Slice(offset, SaltSize));
      offset += SaltSize;

      destinationBody[offset++] = (byte)securityMode;
      if (securityMode == DatagramSecurityMode.Aead)
      {
         BinaryPrimitives.WriteUInt32LittleEndian(destinationBody.Slice(offset, 4), serverKeyId);
         offset += 4;
         serverEcdhPub64.CopyTo(destinationBody.Slice(offset, EcPointSize));
         offset += EcPointSize;
      }

      return offset;
   }

   public static bool TryReadConnectChallenge(
      ReadOnlySpan<byte> body,
      out ReadOnlySpan<byte> cookie80,
      out ReadOnlySpan<byte> serverSalt16,
      out DatagramSecurityMode securityMode,
      out uint serverKeyId,
      out ReadOnlySpan<byte> serverEcdhPub64)
   {
      cookie80 = default;
      serverSalt16 = default;
      securityMode = DatagramSecurityMode.None;
      serverKeyId = 0;
      serverEcdhPub64 = default;

      if (body.Length < CookieSize + SaltSize + 1)
      {
         return false;
      }

      var offset = 0;
      cookie80 = body.Slice(offset, CookieSize);
      offset += CookieSize;
      serverSalt16 = body.Slice(offset, SaltSize);
      offset += SaltSize;
      securityMode = (DatagramSecurityMode)body[offset++];

      if (!Enum.IsDefined(securityMode))
      {
         return false;
      }

      if (securityMode == DatagramSecurityMode.Aead)
      {
         if (offset + 4 + EcPointSize > body.Length) return false;
         serverKeyId = BinaryPrimitives.ReadUInt32LittleEndian(body.Slice(offset, 4));
         offset += 4;
         serverEcdhPub64 = body.Slice(offset, EcPointSize);
      }

      return true;
   }

   public static int WriteConnectAcceptPrefix(
      Span<byte> destinationBody,
      ReadOnlySpan<byte> cookie80,
      ReadOnlySpan<byte> clientSalt16,
      ulong channelConfigHash,
      DatagramSecurityMode securityMode,
      ReadOnlySpan<byte> clientEcdhPub64)
   {
      var offset = 0;
      cookie80.CopyTo(destinationBody.Slice(offset, CookieSize));
      offset += CookieSize;

      clientSalt16.CopyTo(destinationBody.Slice(offset, SaltSize));
      offset += SaltSize;

      BinaryPrimitives.WriteUInt64LittleEndian(destinationBody.Slice(offset, 8), channelConfigHash);
      offset += 8;

      destinationBody[offset++] = (byte)securityMode;
      if (securityMode == DatagramSecurityMode.Aead)
      {
         clientEcdhPub64.CopyTo(destinationBody.Slice(offset, EcPointSize));
         offset += EcPointSize;
      }

      return offset;
   }

   public static bool TryReadConnectAcceptPrefix(
      ReadOnlySpan<byte> body,
      out ReadOnlySpan<byte> cookie80,
      out ReadOnlySpan<byte> clientSalt16,
      out ulong channelConfigHash,
      out DatagramSecurityMode securityMode,
      out ReadOnlySpan<byte> clientEcdhPub64,
      out int prefixLength)
   {
      cookie80 = default;
      clientSalt16 = default;
      channelConfigHash = 0;
      securityMode = DatagramSecurityMode.None;
      clientEcdhPub64 = default;
      prefixLength = 0;

      if (body.Length < CookieSize + SaltSize + 8 + 1)
      {
         return false;
      }

      var offset = 0;
      cookie80 = body.Slice(offset, CookieSize);
      offset += CookieSize;
      clientSalt16 = body.Slice(offset, SaltSize);
      offset += SaltSize;
      channelConfigHash = BinaryPrimitives.ReadUInt64LittleEndian(body.Slice(offset, 8));
      offset += 8;
      securityMode = (DatagramSecurityMode)body[offset++];

      if (!Enum.IsDefined(securityMode))
      {
         return false;
      }

      if (securityMode == DatagramSecurityMode.Aead)
      {
         if (offset + EcPointSize > body.Length) return false;
         clientEcdhPub64 = body.Slice(offset, EcPointSize);
         offset += EcPointSize;
      }

      prefixLength = offset;
      return true;
   }

   public static int WriteConnectAcceptTail(Span<byte> destination, ReadOnlySpan<byte> metadata)
   {
      BinaryPrimitives.WriteUInt16LittleEndian(destination[0..2], (ushort)metadata.Length);
      if (!metadata.IsEmpty)
      {
         metadata.CopyTo(destination.Slice(2, metadata.Length));
      }
      return 2 + metadata.Length;
   }

   public static bool TryReadConnectAcceptTail(ReadOnlySpan<byte> tail, out ReadOnlySpan<byte> metadata)
   {
      metadata = default;
      if (tail.Length < 2)
      {
         return false;
      }

      var len = BinaryPrimitives.ReadUInt16LittleEndian(tail[0..2]);
      if (2 + len > tail.Length)
      {
         return false;
      }

      metadata = tail.Slice(2, len);
      return true;
   }

   public static int WriteConnectAck(
      Span<byte> destinationBody,
      bool accepted,
      uint assignedPeerId,
      uint assignedSessionToken,
      ReadOnlySpan<byte> rejectReason)
   {
      destinationBody[0] = accepted ? (byte)1 : (byte)0;
      BinaryPrimitives.WriteUInt32LittleEndian(destinationBody[1..5], assignedPeerId);
      BinaryPrimitives.WriteUInt32LittleEndian(destinationBody[5..9], assignedSessionToken);
      BinaryPrimitives.WriteUInt16LittleEndian(destinationBody[9..11], (ushort)rejectReason.Length);
      if (!rejectReason.IsEmpty)
      {
         rejectReason.CopyTo(destinationBody.Slice(11, rejectReason.Length));
      }
      return 11 + rejectReason.Length;
   }

   public static bool TryReadConnectAck(
      ReadOnlySpan<byte> body,
      out bool accepted,
      out uint assignedPeerId,
      out uint assignedSessionToken,
      out ReadOnlySpan<byte> rejectReason)
   {
      accepted = false;
      assignedPeerId = 0;
      assignedSessionToken = 0;
      rejectReason = default;

      if (body.Length < 11)
      {
         return false;
      }

      accepted = body[0] != 0;
      assignedPeerId = BinaryPrimitives.ReadUInt32LittleEndian(body[1..5]);
      assignedSessionToken = BinaryPrimitives.ReadUInt32LittleEndian(body[5..9]);
      var reasonLen = BinaryPrimitives.ReadUInt16LittleEndian(body[9..11]);
      if (11 + reasonLen > body.Length)
      {
         return false;
      }

      rejectReason = body.Slice(11, reasonLen);
      return true;
   }
}
