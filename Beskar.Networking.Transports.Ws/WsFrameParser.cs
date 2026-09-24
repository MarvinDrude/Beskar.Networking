using System.Buffers;
using System.Buffers.Binary;
using System.IO.Pipelines;
using System.Numerics;
using System.Runtime.InteropServices;
using System.Security.Cryptography;
using Beskar.Memory.Owners;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Networking.Transports.Common.Options;
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

   private readonly Action<WsNetworkSession, ReadOnlySequence<byte>, WebSocketOpcode>? _onMessage;
   private readonly Func<WsNetworkSession, ReadOnlySequence<byte>, WebSocketOpcode, ValueTask>? _onMessageAsync;

   private readonly Func<WsNetworkSession?>? _sessionProvider;
   private WsNetworkSession? _session;

   private readonly Pipe? _inputPipe;
   private readonly Pipe? _outputPipe;

   private readonly CancellationTokenSource _cts = new();

   private readonly Task _readTask;
   private readonly Task? _writeTask;
   private readonly AsyncLock _writeLock = new();
   private int _disposed;
   private byte _currentFrameOpcode;
   private volatile byte _lastReceivedOpcode = (byte)WebSocketOpcode.Binary;

   private readonly TimeSpan _keepAliveInterval;
   private readonly Task? _pingTask;

   private static readonly Pipe DummyPipe = new();
   public PipeReader Input => _inputPipe?.Reader ?? DummyPipe.Reader;
   public PipeWriter Output => _outputPipe?.Writer ?? DummyPipe.Writer;

   public WsDuplexPipe(IDuplexPipe tcpPipe, INetworkSession tcpSession, bool maskOutgoing, WsTransportOptions options,
      Func<WsNetworkSession?>? sessionProvider = null)
   {
      _tcpPipe = tcpPipe;
      _tcpSession = tcpSession;
      _maskOutgoing = maskOutgoing;
      _maxFrameSize = options.MaxFrameSize;
      _expectMask = !maskOutgoing;
      _onMessage = options.OnMessage;
      _onMessageAsync = options.OnMessageAsync;
      _sessionProvider = sessionProvider;
      _keepAliveInterval = options.KeepAliveInterval;

      if (_onMessage == null && _onMessageAsync == null)
      {
         var memoryPool = SharedTransportMemoryPool.GetNext();
         var pipeOpts = new PipeOptions(
            memoryPool, PipeScheduler.ThreadPool, PipeScheduler.ThreadPool,
            pauseWriterThreshold: 65536, resumeWriterThreshold: 32768,
            useSynchronizationContext: false);

         _inputPipe = new Pipe(pipeOpts);
         _outputPipe = new Pipe(pipeOpts);

         _writeTask = Task.Run(WriteLoopAsync);
      }

      _tcpSession.SessionClosedToken.Register(() =>
      {
         try
         {
            _cts.Cancel();
         }
         catch
         {
            // Ignored
         }
      });

      _readTask = Task.Run(ReadLoopAsync);
      _pingTask = _keepAliveInterval > TimeSpan.Zero ? Task.Run(PingLoopAsync) : null;
   }

   public void SetSession(WsNetworkSession session)
   {
      _session = session;
   }

   public ValueTask SendFrameDirectAsync(ReadOnlySequence<byte> payload, WebSocketOpcode opcode = WebSocketOpcode.Binary, CancellationToken cancellationToken = default)
   {
      var lockTask = _writeLock.LockAsync(cancellationToken);
      if (lockTask.IsCompletedSuccessfully)
      {
         var releaser = lockTask.Result;
         try
         {
            WriteFrame(_tcpPipe.Output, opcode, payload, _maskOutgoing);
            var flushTask = _tcpPipe.Output.FlushAsync(cancellationToken);
            if (flushTask.IsCompletedSuccessfully)
            {
               releaser.Dispose();
               return default;
            }

            return AwaitFlushAndReleaseAsync(flushTask, releaser);
         }
         catch
         {
            releaser.Dispose();
            throw;
         }
      }

      return AwaitLockAndSendAsync(lockTask, payload, opcode, cancellationToken);
   }

   private static async ValueTask AwaitFlushAndReleaseAsync(ValueTask<FlushResult> flushTask, LockReleaser releaser)
   {
      try
      {
         await flushTask.ConfigureAwait(false);
      }
      finally
      {
         releaser.Dispose();
      }
   }

   private async ValueTask AwaitLockAndSendAsync(ValueTask<LockReleaser> lockTask, ReadOnlySequence<byte> payload, WebSocketOpcode opcode, CancellationToken cancellationToken)
   {
      var releaser = await lockTask.ConfigureAwait(false);
      try
      {
         WriteFrame(_tcpPipe.Output, opcode, payload, _maskOutgoing);
         await _tcpPipe.Output.FlushAsync(cancellationToken).ConfigureAwait(false);
      }
      finally
      {
         releaser.Dispose();
      }
   }


   private async Task ReadLoopAsync()
   {
      var reader = _tcpPipe.Input;
      var writer = _inputPipe?.Writer;

      try
      {
         while (!_cts.Token.IsCancellationRequested)
         {
            var result = await reader.ReadAsync(_cts.Token);
            var buffer = result.Buffer;

            while (TryParseFrame(ref buffer, out var opcode,
                      out var payload, out var maskKey, out var isMasked, out var isFin, _maxFrameSize, _expectMask))
            {
               if (opcode is (byte)WebSocketOpcode.Binary or (byte)WebSocketOpcode.Text)
               {
                  _lastReceivedOpcode = opcode;
                  if (_currentFrameOpcode != 0)
                  {
                     throw new InvalidDataException(
                        $"Received a new message starting frame (opcode: {opcode}) while an existing fragmented message (opcode: {_currentFrameOpcode}) is still incomplete.");
                  }

                  if (!isFin)
                  {
                     _currentFrameOpcode = opcode;
                  }

                  if (isMasked && !payload.IsEmpty)
                  {
                     UnmaskInPlace(payload, maskKey);
                  }

                  var currentSession = _sessionProvider?.Invoke() ?? _session;
                  if (_onMessageAsync != null && currentSession != null)
                  {
                     var task = _onMessageAsync(currentSession, payload, (WebSocketOpcode)opcode);
                     if (!task.IsCompletedSuccessfully)
                     {
                        await task;
                     }
                  }
                  else if (_onMessage != null && currentSession != null)
                  {
                     _onMessage(currentSession, payload, (WebSocketOpcode)opcode);
                  }
                  else if (writer != null)
                  {
                     foreach (var segment in payload)
                     {
                        writer.Write(segment.Span);
                     }

                     await writer.FlushAsync(_cts.Token);
                  }
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

                  if (isMasked && !payload.IsEmpty)
                  {
                     UnmaskInPlace(payload, maskKey);
                  }

                  var currentSession = _sessionProvider?.Invoke() ?? _session;
                  if (_onMessageAsync != null && currentSession != null)
                  {
                     var task = _onMessageAsync(currentSession, payload, (WebSocketOpcode)opcode);
                     if (!task.IsCompletedSuccessfully)
                     {
                        await task;
                     }
                  }
                  else if (_onMessage != null && currentSession != null)
                  {
                     _onMessage(currentSession, payload, (WebSocketOpcode)opcode);
                  }
                  else if (writer != null)
                  {
                     foreach (var segment in payload)
                     {
                        writer.Write(segment.Span);
                     }

                     await writer.FlushAsync(_cts.Token);
                  }
               }
               else if (opcode == (byte)WebSocketOpcode.Ping)
               {
                  using (await _writeLock.LockAsync(_cts.Token))
                  {
                     if (isMasked && !payload.IsEmpty)
                     {
                        UnmaskInPlace(payload, maskKey);
                     }

                     WriteFrame(_tcpPipe.Output, WebSocketOpcode.Pong, payload, _maskOutgoing);
                     await _tcpPipe.Output.FlushAsync(_cts.Token);
                  }
               }
               else if (opcode == (byte)WebSocketOpcode.Pong)
               {
                  // Pong frame received in response to client/server ping. No action needed.
               }
               else if (opcode == (byte)WebSocketOpcode.Close)
               {
                  try
                  {
                     using (await _writeLock.LockAsync(CancellationToken.None))
                     {
                        if (isMasked && !payload.IsEmpty)
                        {
                           UnmaskInPlace(payload, maskKey);
                        }

                        WriteFrame(_tcpPipe.Output, WebSocketOpcode.Close, payload, _maskOutgoing);
                        await _tcpPipe.Output.FlushAsync(CancellationToken.None);
                     }
                  }
                  catch
                  {
                     /* Ignored */
                  }

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

         if (writer != null)
         {
            await writer.CompleteAsync();
         }
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
      if (_outputPipe == null) return;
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
                  var outgoingOpcode = (WebSocketOpcode)_lastReceivedOpcode;
                  if (outgoingOpcode is not (WebSocketOpcode.Text or WebSocketOpcode.Binary))
                  {
                     outgoingOpcode = WebSocketOpcode.Binary;
                  }

                  while (!remaining.IsEmpty)
                  {
                     var chunkSize = Math.Min(remaining.Length, maxFrameSize);
                     var chunk = remaining.Slice(0, chunkSize);

                     WriteFrame(writer, outgoingOpcode, chunk, _maskOutgoing);
                     remaining = remaining.Slice(chunkSize);
                  }

                  await writer.FlushAsync(_cts.Token);
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
      out uint maskKey,
      out bool isMasked,
      out bool isFin,
      int maxFrameSize,
      bool expectMask)
   {
      opcode = 0;
      payload = default;
      maskKey = 0;
      isMasked = false;
      isFin = false;

      if (buffer.Length < 2) return false;

      if (buffer.IsSingleSegment)
      {
         var span = buffer.FirstSpan;
         var byte1 = span[0];
         var byte2 = span[1];

         isFin = (byte1 & 0x80) != 0;
         opcode = (byte)(byte1 & 0x0F);

         isMasked = (byte2 & 0x80) != 0;
         var rawLen = byte2 & 0x7F;

         var headerLen = 2;
         long payloadLen;

         if (rawLen == 126)
         {
            if (span.Length < 4) return false;
            payloadLen = BinaryPrimitives.ReadUInt16BigEndian(span.Slice(2, 2));
            headerLen = 4;
         }
         else if (rawLen == 127)
         {
            if (span.Length < 10) return false;
            payloadLen = BinaryPrimitives.ReadInt64BigEndian(span.Slice(2, 8));
            headerLen = 10;

            if (payloadLen < 0 || payloadLen > maxFrameSize)
            {
               throw new InvalidDataException(
                  $"WebSocket frame payload length {payloadLen} is invalid or exceeds the maximum allowed size of {maxFrameSize} bytes.");
            }
         }
         else
         {
            payloadLen = rawLen;
         }

         if (payloadLen < 0 || payloadLen > maxFrameSize)
         {
            throw new InvalidDataException(
               $"WebSocket frame payload length {payloadLen} is invalid or exceeds the maximum allowed size of {maxFrameSize} bytes.");
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
            if (span.Length < headerLen + 4) return false;
            maskKey = BinaryPrimitives.ReadUInt32BigEndian(span.Slice(headerLen, 4));
            headerLen += 4;
         }

         if (span.Length < headerLen + payloadLen)
            return false;

         payload = buffer.Slice(headerLen, payloadLen);
         buffer = buffer.Slice(headerLen + payloadLen);

         return true;
      }

      var reader = new SequenceReader<byte>(buffer);
      reader.TryRead(out var b1);
      reader.TryRead(out var b2);

      isFin = (b1 & 0x80) != 0;
      opcode = (byte)(b1 & 0x0F);

      isMasked = (b2 & 0x80) != 0;
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
         if (!reader.TryReadBigEndian(out int maskVal))
            return false;

         maskKey = (uint)maskVal;
      }

      if (reader.Remaining < len)
         return false;

      payload = buffer.Slice(reader.Position, len);
      buffer = buffer.Slice(buffer.GetPosition(len, reader.Position));

      return true;
   }

   private static void UnmaskInPlace(ReadOnlySequence<byte> payload, uint maskKey)
   {
      Span<byte> maskSpan = stackalloc byte[4];
      BinaryPrimitives.WriteUInt32BigEndian(maskSpan, maskKey);

      var payloadIndex = 0;
      if (payload.IsSingleSegment)
      {
         var mem = payload.First;
         if (!mem.IsEmpty)
         {
            var span = MemoryMarshal.CreateSpan(ref MemoryMarshal.GetReference(mem.Span), mem.Length);
            MaskOrUnmask(span, span, maskSpan, ref payloadIndex);
         }

         return;
      }

      foreach (var segment in payload)
      {
         if (segment.IsEmpty) continue;
         var span = MemoryMarshal.CreateSpan(ref MemoryMarshal.GetReference(segment.Span), segment.Length);
         MaskOrUnmask(span, span, maskSpan, ref payloadIndex);
      }
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
            vectorMaskBytes[i] = maskKey[(payloadIndex + i) & 3];
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
            target[i] = (byte)(source[i] ^ maskKey[(payloadIndex++) & 3]);
         }
      }
      else
      {
         var i = 0;
         if ((payloadIndex & 3) == 0 && len >= 4)
         {
            var mask32 = BinaryPrimitives.ReadUInt32LittleEndian(maskKey);
            while (len - i >= 4)
            {
               var src32 = BinaryPrimitives.ReadUInt32LittleEndian(source.Slice(i, 4));
               BinaryPrimitives.WriteUInt32LittleEndian(target.Slice(i, 4), src32 ^ mask32);
               i += 4;
            }
            payloadIndex += i;
         }

         for (; i < len; i++)
         {
            target[i] = (byte)(source[i] ^ maskKey[(payloadIndex++) & 3]);
         }
      }
   }

   private static void WriteFrame(
      PipeWriter tcpWriter,
      WebSocketOpcode opcode,
      ReadOnlySpan<byte> payload,
      bool mask)
   {
      var len = payload.Length;
      var headerSize = 2;

      if (len >= 65536) headerSize += 8;
      else if (len >= 126) headerSize += 2;

      if (mask) headerSize += 4;

      if (!mask)
      {
         var totalSize = headerSize + len;
         var span = tcpWriter.GetSpan(totalSize);
         span[0] = (byte)(0x80 | (byte)opcode);

         if (len < 126)
         {
            span[1] = (byte)len;
         }
         else if (len < 65536)
         {
            span[1] = 126;
            BinaryPrimitives.WriteUInt16BigEndian(span.Slice(2, 2), (ushort)len);
         }
         else
         {
            span[1] = 127;
            BinaryPrimitives.WriteInt64BigEndian(span.Slice(2, 8), len);
         }

         if (len > 0)
         {
            payload.CopyTo(span.Slice(headerSize, len));
         }

         tcpWriter.Advance(totalSize);
         return;
      }

      var headerSpan = tcpWriter.GetSpan(headerSize);
      headerSpan[0] = (byte)(0x80 | (byte)opcode);
      var index = 2;

      if (len < 126)
      {
         headerSpan[1] = (byte)(0x80 | (byte)len);
      }
      else if (len < 65536)
      {
         headerSpan[1] = 0x80 | 126;
         BinaryPrimitives.WriteUInt16BigEndian(headerSpan.Slice(2, 2), (ushort)len);
         index = 4;
      }
      else
      {
         headerSpan[1] = 0x80 | 127;
         BinaryPrimitives.WriteInt64BigEndian(headerSpan.Slice(2, 8), len);
         index = 10;
      }

      Span<byte> maskKey = stackalloc byte[4];
      RandomNumberGenerator.Fill(maskKey);

      headerSpan[index++] = maskKey[0];
      headerSpan[index++] = maskKey[1];
      headerSpan[index++] = maskKey[2];
      headerSpan[index] = maskKey[3];

      tcpWriter.Advance(headerSize);

      var payloadIndex = 0;
      var remaining = payload;

      while (!remaining.IsEmpty)
      {
         var chunkSize = Math.Min(remaining.Length, 4096);
         var targetSpan = tcpWriter.GetSpan(chunkSize);

         MaskOrUnmask(targetSpan[..chunkSize], remaining[..chunkSize], maskKey, ref payloadIndex);
         tcpWriter.Advance(chunkSize);
         remaining = remaining[chunkSize..];
      }
   }

   private static void WriteFrame(
      PipeWriter tcpWriter,
      WebSocketOpcode opcode,
      ReadOnlySequence<byte> payload,
      bool mask)
   {
      if (payload.IsSingleSegment)
      {
         WriteFrame(tcpWriter, opcode, payload.FirstSpan, mask);
         return;
      }

      var len = payload.Length;
      var headerSize = 2;

      if (len >= 65536) headerSize += 8;
      else if (len >= 126) headerSize += 2;

      if (mask) headerSize += 4;

      if (!mask)
      {
         var totalSize = (int)(headerSize + len);
         var span = tcpWriter.GetSpan(totalSize);
         span[0] = (byte)(0x80 | (byte)opcode);

         if (len < 126)
         {
            span[1] = (byte)len;
         }
         else if (len < 65536)
         {
            span[1] = 126;
            BinaryPrimitives.WriteUInt16BigEndian(span.Slice(2, 2), (ushort)len);
         }
         else
         {
            span[1] = 127;
            BinaryPrimitives.WriteInt64BigEndian(span.Slice(2, 8), len);
         }

         payload.CopyTo(span.Slice(headerSize, (int)len));
         tcpWriter.Advance(totalSize);
         return;
      }

      var headerSpan = tcpWriter.GetSpan(headerSize);
      headerSpan[0] = (byte)(0x80 | (byte)opcode);
      var index = 2;

      if (len < 126)
      {
         headerSpan[1] = (byte)(0x80 | (byte)len);
      }
      else if (len < 65536)
      {
         headerSpan[1] = 0x80 | 126;
         BinaryPrimitives.WriteUInt16BigEndian(headerSpan.Slice(2, 2), (ushort)len);
         index = 4;
      }
      else
      {
         headerSpan[1] = 0x80 | 127;
         BinaryPrimitives.WriteInt64BigEndian(headerSpan.Slice(2, 8), len);
         index = 10;
      }

      Span<byte> maskKey = stackalloc byte[4];
      RandomNumberGenerator.Fill(maskKey);

      headerSpan[index++] = maskKey[0];
      headerSpan[index++] = maskKey[1];
      headerSpan[index++] = maskKey[2];
      headerSpan[index] = maskKey[3];

      tcpWriter.Advance(headerSize);

      var payloadIndex = 0;

      foreach (var segment in payload)
      {
         var segRemaining = segment.Span;
         while (!segRemaining.IsEmpty)
         {
            var chunkSize = Math.Min(segRemaining.Length, 4096);
            var targetSpan = tcpWriter.GetSpan(chunkSize);

            MaskOrUnmask(targetSpan[..chunkSize], segRemaining[..chunkSize], maskKey, ref payloadIndex);
            tcpWriter.Advance(chunkSize);
            segRemaining = segRemaining[chunkSize..];
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
      WriteFrame(tcpWriter, opcode, payload, mask);
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
         if (_writeTask is not null)
         {
            await _writeTask;
         }
      }
      catch
      {
         /* Ignored */
      }

      if (_inputPipe is not null)
      {
         await _inputPipe.Reader.CompleteAsync();
         await _inputPipe.Writer.CompleteAsync();
      }

      if (_outputPipe is not null)
      {
         await _outputPipe.Reader.CompleteAsync();
         await _outputPipe.Writer.CompleteAsync();
      }

      _cts.Dispose();
   }
}
