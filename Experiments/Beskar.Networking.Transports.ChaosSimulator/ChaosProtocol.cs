using System;
using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Threading;
using System.Threading.Tasks;

namespace Beskar.Networking.Transports.ChaosSimulator;

public record ChaosPacket(int SequenceNumber, long Timestamp, byte[] Payload, bool IsCorrupted = false)
{
   public static uint ComputeChecksum(int sequenceNumber, long timestamp, ReadOnlySpan<byte> payload)
   {
      // FNV-1a 32-bit hash algorithm (extremely fast, zero allocation, sensitive to bit flips)
      var hash = 2166136261;

      hash = (hash ^ (uint)sequenceNumber) * 16777619;

      hash = (hash ^ (uint)(timestamp & 0xFFFFFFFF)) * 16777619;
      hash = (hash ^ (uint)(timestamp >> 32)) * 16777619;

      for (var i = 0; i < payload.Length; i++)
      {
         hash = (hash ^ payload[i]) * 16777619;
      }

      return hash;
   }

   public static async ValueTask WriteAsync(PipeWriter writer, int sequenceNumber, byte[] payload, CancellationToken ct)
   {
      var timestamp = DateTimeOffset.UtcNow.Ticks;
      var checksum = ComputeChecksum(sequenceNumber, timestamp, payload);
      var length = 16 + payload.Length; // 4 (Seq) + 4 (Checksum) + 8 (Timestamp) + Payload

      // Write Header: 4 (Length) + 4 (Seq) + 4 (Checksum) + 8 (Timestamp) = 20 bytes
      var span = writer.GetSpan(20);
      BinaryPrimitives.WriteInt32LittleEndian(span[..4], length);
      BinaryPrimitives.WriteInt32LittleEndian(span[4..8], sequenceNumber);
      BinaryPrimitives.WriteUInt32LittleEndian(span[8..12], checksum);
      BinaryPrimitives.WriteInt64LittleEndian(span[12..20], timestamp);

      writer.Advance(20);

      // Write Payload
      if (payload.Length > 0)
      {
         await writer.WriteAsync(payload, ct);
      }
      else
      {
         await writer.FlushAsync(ct);
      }
   }

   public static async ValueTask<ChaosPacket?> ReadAsync(PipeReader reader, CancellationToken ct)
   {
      while (true)
      {
         var result = await reader.ReadAsync(ct);
         var buffer = result.Buffer;

         if (buffer.Length < 4)
         {
            if (result.IsCompleted) return null;
            reader.AdvanceTo(buffer.Start, buffer.End);
            continue;
         }

         var packet = CreatePacket(reader, buffer, result, ct);
         if (packet != null) return packet;
      }
   }

   private static ChaosPacket? CreatePacket(
      PipeReader reader, ReadOnlySequence<byte> buffer,
      ReadResult result, CancellationToken ct)
   {
      Span<byte> lengthSpan = stackalloc byte[4];
      buffer.Slice(0, 4).CopyTo(lengthSpan);
      var length = BinaryPrimitives.ReadInt32LittleEndian(lengthSpan);

      if (length is < 16 or > 10 * 1024 * 1024)
      {
         reader.AdvanceTo(buffer.End);
         throw new InvalidOperationException($"Invalid packet length: {length}. Connection framing lost.");
      }

      var totalPacketSize = 4 + length;
      if (buffer.Length < totalPacketSize)
      {
         if (result.IsCompleted) return null;
         reader.AdvanceTo(buffer.Start, buffer.End);
         return null;
      }

      var packetBuffer = buffer.Slice(4, length);

      Span<byte> headerSpan = stackalloc byte[16];
      packetBuffer.Slice(0, 16).CopyTo(headerSpan);

      var sequenceNumber = BinaryPrimitives.ReadInt32LittleEndian(headerSpan[..4]);
      var checksum = BinaryPrimitives.ReadUInt32LittleEndian(headerSpan[4..8]);
      var timestamp = BinaryPrimitives.ReadInt64LittleEndian(headerSpan[8..16]);

      var payloadLength = length - 16;
      var payload = new byte[payloadLength];
      if (payloadLength > 0)
      {
         packetBuffer.Slice(16, payloadLength).CopyTo(payload);
      }

      var computed = ComputeChecksum(sequenceNumber, timestamp, payload);
      var isCorrupted = computed != checksum;

      reader.AdvanceTo(buffer.GetPosition(totalPacketSize));
      return new ChaosPacket(sequenceNumber, timestamp, payload, isCorrupted);
   }
}
