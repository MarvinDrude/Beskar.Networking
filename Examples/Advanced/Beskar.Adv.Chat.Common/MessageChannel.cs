using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;

namespace Beskar.Adv.Chat.Common;

public sealed class MessageChannel(IDuplexPipe transport)
{
   private readonly PipeReader _reader = transport.Input;
   private readonly PipeWriter _writer = transport.Output;

   public async Task WritePacketAsync(ChatPacket packet, CancellationToken ct = default)
   {
      var payloadLength = packet.Payload.Length;
      var totalLength = 1 + payloadLength; // 1 byte for Type + payload length

      var memory = _writer.GetMemory(4 + totalLength);
      BinaryPrimitives.WriteInt32BigEndian(memory.Span[..4], totalLength);

      memory.Span[4] = (byte)packet.Type;
      packet.Payload.CopyTo(memory.Span[5..]);

      _writer.Advance(4 + totalLength);
      await _writer.FlushAsync(ct);
   }

   public async Task<ChatPacket?> ReadPacketAsync(CancellationToken ct = default)
   {
      try
      {
         while (!ct.IsCancellationRequested)
         {
            var result = await _reader.ReadAsync(ct);
            var buffer = result.Buffer;
            if (FrameParser.TryParseFrame(ref buffer, out var packet, out var consumedPosition))
            {
               _reader.AdvanceTo(consumedPosition, consumedPosition);
               return packet;
            }
            _reader.AdvanceTo(buffer.Start, buffer.End);
            if (result.IsCompleted || result.IsCanceled)
            {
               return null;
            }
         }
      }
      catch (InvalidOperationException ex) when (ex.Message.Contains("completed"))
      {
         return null;
      }
      catch (OperationCanceledException)
      {
         return null;
      }

      return null;
   }
}
