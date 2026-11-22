using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;

namespace Beskar.Networking.Datagram.Nat;

/// <summary>
/// Wire codec for lightweight NAT hole punching probes and rendezvous signaling packets.
/// </summary>
internal static class DatagramRendezvousCodec
{
   public static int WritePunchProbe(Span<byte> dest, ulong punchToken, bool isReply)
   {
      BinaryPrimitives.WriteUInt64LittleEndian(dest[0..8], punchToken);
      dest[8] = isReply ? (byte)1 : (byte)0;
      return 9;
   }

   public static bool TryReadPunchProbe(ReadOnlySpan<byte> body, out ulong punchToken, out bool isReply)
   {
      punchToken = 0;
      isReply = false;
      if (body.Length < 9) return false;

      punchToken = BinaryPrimitives.ReadUInt64LittleEndian(body[0..8]);
      isReply = body[8] != 0;
      return true;
   }

   public static int WriteRegister(Span<byte> dest, ReadOnlySpan<byte> nodeId, IPEndPoint? lanHint)
   {
      var offset = 0;
      dest[offset++] = (byte)nodeId.Length;
      nodeId.CopyTo(dest.Slice(offset, nodeId.Length));
      offset += nodeId.Length;

      offset += WriteOptionalEndPoint(dest[offset..], lanHint);
      return offset;
   }

   public static bool TryReadRegister(
      ReadOnlySpan<byte> body,
      out ReadOnlySpan<byte> nodeId,
      out IPEndPoint? lanHint)
   {
      nodeId = default;
      lanHint = null;
      if (body.Length < 2) return false;

      var len = body[0];
      if (len == 0 || len > 64 || 1 + len >= body.Length) return false;

      nodeId = body.Slice(1, len);
      return TryReadOptionalEndPoint(body[(1 + len)..], out lanHint, out _);
   }

   public static int WriteMapped(Span<byte> dest, ReadOnlySpan<byte> nodeId, IPEndPoint publicEndPoint)
   {
      var offset = 0;
      dest[offset++] = (byte)nodeId.Length;
      nodeId.CopyTo(dest.Slice(offset, nodeId.Length));
      offset += nodeId.Length;

      offset += WriteEndPoint(dest[offset..], publicEndPoint);
      return offset;
   }

   public static bool TryReadMapped(
      ReadOnlySpan<byte> body,
      out ReadOnlySpan<byte> nodeId,
      out IPEndPoint? publicEndPoint)
   {
      nodeId = default;
      publicEndPoint = null;
      if (body.Length < 2) return false;

      var len = body[0];
      if (len == 0 || len > 64 || 1 + len >= body.Length) return false;

      nodeId = body.Slice(1, len);
      return TryReadEndPoint(body[(1 + len)..], out publicEndPoint, out _);
   }

   public static int WriteIntroduceRequest(
      Span<byte> dest,
      ReadOnlySpan<byte> requesterNodeId,
      ReadOnlySpan<byte> targetNodeId,
      ulong punchToken)
   {
      var offset = 0;
      dest[offset++] = (byte)requesterNodeId.Length;
      requesterNodeId.CopyTo(dest.Slice(offset, requesterNodeId.Length));
      offset += requesterNodeId.Length;

      dest[offset++] = (byte)targetNodeId.Length;
      targetNodeId.CopyTo(dest.Slice(offset, targetNodeId.Length));
      offset += targetNodeId.Length;

      BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(offset, 8), punchToken);
      offset += 8;
      return offset;
   }

   public static bool TryReadIntroduceRequest(
      ReadOnlySpan<byte> body,
      out ReadOnlySpan<byte> requesterNodeId,
      out ReadOnlySpan<byte> targetNodeId,
      out ulong punchToken)
   {
      requesterNodeId = default;
      targetNodeId = default;
      punchToken = 0;

      if (body.Length < 10) return false;
      var offset = 0;

      var reqLen = body[offset++];
      if (reqLen == 0 || reqLen > 64 || offset + reqLen >= body.Length) return false;
      requesterNodeId = body.Slice(offset, reqLen);
      offset += reqLen;

      var tgtLen = body[offset++];
      if (tgtLen == 0 || tgtLen > 64 || offset + tgtLen + 8 > body.Length) return false;
      targetNodeId = body.Slice(offset, tgtLen);
      offset += tgtLen;

      punchToken = BinaryPrimitives.ReadUInt64LittleEndian(body.Slice(offset, 8));
      return true;
   }

   public static int WriteIntroduceNotify(
      Span<byte> dest,
      ReadOnlySpan<byte> peerNodeId,
      ulong punchToken,
      IPEndPoint peerPublicEndPoint,
      IPEndPoint? peerLanEndPoint)
   {
      var offset = 0;
      dest[offset++] = (byte)peerNodeId.Length;
      peerNodeId.CopyTo(dest.Slice(offset, peerNodeId.Length));
      offset += peerNodeId.Length;

      BinaryPrimitives.WriteUInt64LittleEndian(dest.Slice(offset, 8), punchToken);
      offset += 8;

      offset += WriteEndPoint(dest[offset..], peerPublicEndPoint);
      offset += WriteOptionalEndPoint(dest[offset..], peerLanEndPoint);
      return offset;
   }

   public static bool TryReadIntroduceNotify(
      ReadOnlySpan<byte> body,
      out ReadOnlySpan<byte> peerNodeId,
      out ulong punchToken,
      out IPEndPoint? peerPublicEndPoint,
      out IPEndPoint? peerLanEndPoint)
   {
      peerNodeId = default;
      punchToken = 0;
      peerPublicEndPoint = null;
      peerLanEndPoint = null;

      if (body.Length < 10) return false;
      var offset = 0;

      var idLen = body[offset++];
      if (idLen == 0 || idLen > 64 || offset + idLen + 8 >= body.Length) return false;
      peerNodeId = body.Slice(offset, idLen);
      offset += idLen;

      punchToken = BinaryPrimitives.ReadUInt64LittleEndian(body.Slice(offset, 8));
      offset += 8;

      if (!TryReadEndPoint(body[offset..], out peerPublicEndPoint, out var epBytes)) return false;
      offset += epBytes;

      return TryReadOptionalEndPoint(body[offset..], out peerLanEndPoint, out _);
   }

   private static int WriteOptionalEndPoint(Span<byte> dest, IPEndPoint? ep)
   {
      if (ep is null)
      {
         dest[0] = 0;
         return 1;
      }

      dest[0] = 1;
      return 1 + WriteEndPoint(dest[1..], ep);
   }

   private static bool TryReadOptionalEndPoint(ReadOnlySpan<byte> src, out IPEndPoint? ep, out int consumed)
   {
      ep = null;
      consumed = 0;
      if (src.IsEmpty) return false;

      if (src[0] == 0)
      {
         consumed = 1;
         return true;
      }

      if (!TryReadEndPoint(src[1..], out ep, out var inner)) return false;
      consumed = 1 + inner;
      return true;
   }

   private static int WriteEndPoint(Span<byte> dest, IPEndPoint ep)
   {
      var isV6 = ep.AddressFamily == AddressFamily.InterNetworkV6;
      dest[0] = isV6 ? (byte)6 : (byte)4;
      ep.Address.TryWriteBytes(dest[1..], out var written);
      BinaryPrimitives.WriteUInt16LittleEndian(dest.Slice(1 + written, 2), (ushort)ep.Port);

      return 1 + written + 2;
   }

   private static bool TryReadEndPoint(ReadOnlySpan<byte> src, out IPEndPoint? ep, out int consumed)
   {
      ep = null;
      consumed = 0;
      if (src.IsEmpty) return false;

      var ipLen = src[0] switch
      {
         4 => 4,
         6 => 16,
         _ => 0
      };

      if (ipLen == 0 || src.Length < 1 + ipLen + 2) return false;

      var ip = new IPAddress(src.Slice(1, ipLen));
      var port = BinaryPrimitives.ReadUInt16LittleEndian(src.Slice(1 + ipLen, 2));

      ep = new IPEndPoint(ip, port);
      consumed = 1 + ipLen + 2;

      return true;
   }
}
