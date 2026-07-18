using System.Buffers;
using System.IO.Pipelines;
using System.Numerics;
using System.Security.Cryptography;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Networking.Transports.Ws.Enums;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Transports.Ws;

/// <summary>
/// A high-performance, zero-allocation custom duplex pipeline that handles WebSocket framing
/// on top of an underlying TCP duplex pipe.
/// </summary>
public sealed class WsDuplexPipe : IDuplexPipe, IAsyncDisposable
{
   private readonly IDuplexPipe _tcpPipe;
   private readonly INetworkSession _tcpSession;
   private readonly bool _maskOutgoing;
   private readonly int _maxFrameSize;
   private readonly bool _expectMask;

   private readonly Pipe _inputPipe = new();
   private readonly Pipe _outputPipe = new();

   private readonly CancellationTokenSource _cts = new();

   private readonly Task _readTask;
   private readonly Task _writeTask;
   private readonly AsyncLock _writeLock = new();
   private int _disposed;
   private byte _currentFrameOpcode;

   private readonly TimeSpan _keepAliveInterval;
   private readonly Task? _pingTask;

   public PipeReader Input => _inputPipe.Reader;
   public PipeWriter Output => _outputPipe.Writer;

   public WsDuplexPipe(IDuplexPipe tcpPipe, INetworkSession tcpSession, bool maskOutgoing, WsTransportOptions options)
   {
      _tcpPipe = tcpPipe;
      _tcpSession = tcpSession;
      _maskOutgoing = maskOutgoing;
      _maxFrameSize = options.MaxFrameSize;
      _expectMask = !maskOutgoing;
      _keepAliveInterval = options.KeepAliveInterval;

      _readTask = Task.Run(ReadLoopAsync);
      _writeTask = Task.Run(WriteLoopAsync);
      _pingTask = _keepAliveInterval > TimeSpan.Zero ? Task.Run(PingLoopAsync) : null;
   }

   private async Task ReadLoopAsync()
   {
      var reader = _tcpPipe.Input;
      var writer = _inputPipe.Writer;

      try
      {
         while (!_cts.Token.IsCancellationRequested)
         {
            var result = await reader.ReadAsync(_cts.Token);
            var buffer = result.Buffer;

            while (TryParseFrame(ref buffer, out var opcode,
                      out var payload, out var maskKey, out var isFin, _maxFrameSize, _expectMask))
            {
               if (opcode is (byte)WebSocketOpcode.Binary or (byte)WebSocketOpcode.Text)
               {
                  if (_currentFrameOpcode != 0)
                  {
                     throw new InvalidDataException(
                        $"Received a new message starting frame (opcode: {opcode}) while an existing fragmented message (opcode: {_currentFrameOpcode}) is still incomplete.");
                  }

                  if (!isFin)
                  {
                     _currentFrameOpcode = opcode;
                  }

                  if (maskKey != null)
                  {
                     UnmaskAndWrite(writer, payload, maskKey);
                  }
                  else
                  {
                     foreach (var segment in payload)
                     {
                        writer.Write(segment.Span);
                     }
                  }

                  await writer.FlushAsync(_cts.Token);
               }
               else if (opcode == 0) // Continuation Frame
               {
                  if (_currentFrameOpcode == 0)
                  {
                     throw new InvalidDataException(
                        "Received an unexpected WebSocket Continuation frame (opcode 0) when no fragmented message was active.");
                  }

                  if (isFin)
                  {
                     _currentFrameOpcode = 0;
                  }

                  if (maskKey != null)
                  {
                     UnmaskAndWrite(writer, payload, maskKey);
                  }
                  else
                  {
                     foreach (var segment in payload)
                     {
                        writer.Write(segment.Span);
                     }
                  }

                  await writer.FlushAsync(_cts.Token);
               }
               else if (opcode == (byte)WebSocketOpcode.Ping)
               {
                  using (await _writeLock.LockAsync(_cts.Token))
                  {
                     await WriteFrameAsync(_tcpPipe.Output, WebSocketOpcode.Pong, payload, _maskOutgoing, _cts.Token);
                  }
               }
               else if (opcode == (byte)WebSocketOpcode.Close)
               {
                  await _cts.CancelAsync();
                  break;
               }
               else
               {
                  throw new InvalidDataException($"Received invalid or unsupported WebSocket opcode: {opcode}");
               }
            }

            reader.AdvanceTo(buffer.Start, buffer.End);

            if (result.IsCompleted || result.IsCanceled)
            {
               break;
            }
         }
      }
      catch (OperationCanceledException)
      {
         // Normal shutdown
      }
      catch (Exception ex)
      {
         if (_maskOutgoing)
         {
            TraceLogger.LogClientError("WS Connection: Error in read loop: {0}", ex.Message);
         }
         else
         {
            TraceLogger.LogServerError("WS Connection: Error in read loop: {0}", ex.Message);
         }
      }
      finally
      {
         try
         {
            await _cts.CancelAsync();
         }
         catch
         {
            /* Ignored */
         }

         await writer.CompleteAsync();
         await reader.CompleteAsync();

         try
         {
            await _tcpSession.DisposeAsync();
         }
         catch
         {
            /* Ignored */
         }
      }
   }

   private async Task WriteLoopAsync()
   {
      var reader = _outputPipe.Reader;
      var writer = _tcpPipe.Output;

      try
      {
         while (!_cts.Token.IsCancellationRequested)
         {
            var result = await reader.ReadAsync(_cts.Token);
            var buffer = result.Buffer;

            if (!buffer.IsEmpty)
            {
               const int maxFrameSize = 65536;
               var remaining = buffer;

               using (await _writeLock.LockAsync(_cts.Token))
               {
                  while (!remaining.IsEmpty)
                  {
                     var chunkSize = Math.Min(remaining.Length, maxFrameSize);
                     var chunk = remaining.Slice(0, chunkSize);

                     await WriteFrameAsync(writer, WebSocketOpcode.Binary, chunk, _maskOutgoing, _cts.Token);
                     remaining = remaining.Slice(chunkSize);
                  }
               }

               reader.AdvanceTo(buffer.End);
            }
            else
            {
               reader.AdvanceTo(buffer.Start, buffer.End);
            }

            if (result.IsCompleted || result.IsCanceled)
            {
               break;
            }
         }
      }
      catch (OperationCanceledException)
      {
         // Normal shutdown
      }
      catch (Exception)
      {
         // Connection reset / error
      }
      finally
      {
         await reader.CompleteAsync();
         await writer.CompleteAsync();
      }
   }

   private static bool TryParseFrame(
      ref ReadOnlySequence<byte> buffer,
      out byte opcode,
      out ReadOnlySequence<byte> payload,
      out byte[]? maskKey,
      out bool isFin,
      int maxFrameSize,
      bool expectMask)
   {
      opcode = 0;
      payload = default;
      maskKey = null;
      isFin = false;

      if (buffer.Length < 2) return false;

      var reader = new SequenceReader<byte>(buffer);
      reader.TryRead(out var b1);
      reader.TryRead(out var b2);

      isFin = (b1 & 0x80) != 0;
      opcode = (byte)(b1 & 0x0F);

      var isMasked = (b2 & 0x80) != 0;
      var len = (long)(b2 & 0x7F);

      if (len == 126)
      {
         if (!reader.TryReadBigEndian(out short len16))
            return false;

         len = (ushort)len16;
      }
      else if (len == 127)
      {
         if (!reader.TryReadBigEndian(out long len64))
            return false;

         if (len64 < 0 || len64 > maxFrameSize)
         {
            throw new InvalidDataException(
               $"WebSocket frame payload length {len64} is invalid or exceeds the maximum allowed size of {maxFrameSize} bytes.");
         }

         len = len64;
      }

      if (len < 0 || len > maxFrameSize)
      {
         throw new InvalidDataException(
            $"WebSocket frame payload length {len} is invalid or exceeds the maximum allowed size of {maxFrameSize} bytes.");
      }

      if (isMasked != expectMask)
      {
         if (expectMask)
         {
            throw new InvalidDataException("Received unmasked WebSocket frame, but server requires masked frames.");
         }
         else
         {
            throw new InvalidDataException("Received masked WebSocket frame, but client requires unmasked frames.");
         }
      }

      if (isMasked)
      {
         maskKey = new byte[4];
         for (var i = 0; i < 4; i++)
         {
            if (!reader.TryRead(out maskKey[i]))
               return false;
         }
      }

      if (reader.Remaining < len)
         return false;

      payload = buffer.Slice(reader.Position, len);
      buffer = buffer.Slice(buffer.GetPosition(len, reader.Position));

      return true;
   }

   private static void MaskOrUnmask(Span<byte> target, ReadOnlySpan<byte> source, ReadOnlySpan<byte> maskKey,
      ref int payloadIndex)
   {
      var len = source.Length;

      if (Vector.IsHardwareAccelerated && len >= Vector<byte>.Count)
      {
         var vectorSize = Vector<byte>.Count;
         Span<byte> vectorMaskBytes = stackalloc byte[vectorSize];

         for (var i = 0; i < vectorSize; i++)
         {
            vectorMaskBytes[i] = maskKey[(payloadIndex + i) % 4];
         }

         var maskVector = new Vector<byte>(vectorMaskBytes);
         var simdLength = len - (len % vectorSize);

         for (var i = 0; i < simdLength; i += vectorSize)
         {
            var sourceVec = new Vector<byte>(source.Slice(i, vectorSize));
            var xorVec = sourceVec ^ maskVector;
            xorVec.CopyTo(target.Slice(i, vectorSize));
         }

         payloadIndex += simdLength;

         for (var i = simdLength; i < len; i++)
         {
            target[i] = (byte)(source[i] ^ maskKey[payloadIndex++ % 4]);
         }
      }
      else
      {
         for (var i = 0; i < len; i++)
         {
            target[i] = (byte)(source[i] ^ maskKey[payloadIndex++ % 4]);
         }
      }
   }

   private static void UnmaskAndWrite(PipeWriter writer, ReadOnlySequence<byte> payload, byte[] maskKey)
   {
      var payloadIndex = 0;

      foreach (var segment in payload)
      {
         var remaining = segment.Span;
         while (!remaining.IsEmpty)
         {
            var chunkSize = Math.Min(remaining.Length, 4096);
            var targetSpan = writer.GetSpan(chunkSize);

            MaskOrUnmask(targetSpan[..chunkSize], remaining[..chunkSize], maskKey, ref payloadIndex);
            writer.Advance(chunkSize);
            remaining = remaining[chunkSize..];
         }
      }
   }

   private static async Task WriteFrameAsync(
      PipeWriter tcpWriter,
      WebSocketOpcode opcode,
      ReadOnlySequence<byte> payload,
      bool mask,
      CancellationToken ct)
   {
      var len = payload.Length;
      var headerSize = 2;

      if (len >= 65536) headerSize += 8;
      else if (len >= 126) headerSize += 2;

      if (mask) headerSize += 4;

      var headerSpan = tcpWriter.GetSpan(headerSize);
      headerSpan[0] = (byte)(0x80 | (byte)opcode);
      var index = 2;

      if (len < 126)
      {
         headerSpan[1] = (byte)((mask ? 0x80 : 0x00) | (byte)len);
      }
      else if (len < 65536)
      {
         headerSpan[1] = (byte)((mask ? 0x80 : 0x00) | 126);
         headerSpan[2] = (byte)(len >> 8);
         headerSpan[3] = (byte)len;

         index = 4;
      }
      else
      {
         headerSpan[1] = (byte)((mask ? 0x80 : 0x00) | 127);

         for (var i = 0; i < 8; i++)
         {
            headerSpan[2 + i] = (byte)(len >> ((7 - i) * 8));
         }

         index = 10;
      }

      Span<byte> maskKey = stackalloc byte[4];
      if (mask)
      {
         RandomNumberGenerator.Fill(maskKey);

         headerSpan[index++] = maskKey[0];
         headerSpan[index++] = maskKey[1];
         headerSpan[index++] = maskKey[2];
         headerSpan[index] = maskKey[3];
      }

      tcpWriter.Advance(headerSize);

      if (mask)
      {
         var payloadIndex = 0;

         foreach (var segment in payload)
         {
            var remaining = segment.Span;
            while (!remaining.IsEmpty)
            {
               var chunkSize = Math.Min(remaining.Length, 4096);
               var targetSpan = tcpWriter.GetSpan(chunkSize);

               MaskOrUnmask(targetSpan[..chunkSize], remaining[..chunkSize], maskKey, ref payloadIndex);
               tcpWriter.Advance(chunkSize);
               remaining = remaining[chunkSize..];
            }
         }
      }
      else
      {
         foreach (var segment in payload)
         {
            tcpWriter.Write(segment.Span);
         }
      }

      await tcpWriter.FlushAsync(ct);
   }

   private async Task PingLoopAsync()
   {
      try
      {
         while (!_cts.Token.IsCancellationRequested)
         {
            await Task.Delay(_keepAliveInterval, _cts.Token);

            using (await _writeLock.LockAsync(_cts.Token))
            {
               await WriteFrameAsync(_tcpPipe.Output, WebSocketOpcode.Ping, ReadOnlySequence<byte>.Empty, _maskOutgoing,
                  _cts.Token);
            }
         }
      }
      catch (OperationCanceledException)
      {
         // Normal shutdown
      }
      catch (Exception ex)
      {
         if (_maskOutgoing)
         {
            TraceLogger.LogClientError("WS Connection: Keep-alive ping failed: {0}", ex.Message);
         }
         else
         {
            TraceLogger.LogServerError("WS Connection: Keep-alive ping failed: {0}", ex.Message);
         }
      }
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      try
      {
         await _cts.CancelAsync();
      }
      catch
      {
         // Ignored
      }

      try
      {
         if (_pingTask is not null)
         {
            await _pingTask;
         }
      }
      catch
      {
         /* Ignored */
      }

      try
      {
         await _readTask;
      }
      catch
      {
         /* Ignored */
      }

      try
      {
         await _writeTask;
      }
      catch
      {
         /* Ignored */
      }

      await _inputPipe.Reader.CompleteAsync();
      await _inputPipe.Writer.CompleteAsync();
      await _outputPipe.Reader.CompleteAsync();
      await _outputPipe.Writer.CompleteAsync();

      _cts.Dispose();
   }
}
