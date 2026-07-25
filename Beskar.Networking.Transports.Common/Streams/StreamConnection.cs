using System.Buffers;
using System.IO.Pipelines;
using Beskar.Networking.Abstractions.Interfaces.Pools;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Pools;
using Beskar.Networking.Transports.Common.Pipelines;

namespace Beskar.Networking.Transports.Common.Streams;

public sealed class StreamConnection(
   PipeOptions readOptions,
   PipeOptions writeOptions)
   : IDuplexPipe, IAsyncDisposable, IPooledObject
{
   private static readonly int MinAllocBufferSize = NetworkPinnedBlockMemoryPool.BlockSize / 2;

   private Pipe _readPipe = new(readOptions);
   private Pipe _writePipe = new(writeOptions);

   private Task? _readTask;
   private Task? _writeTask;

   private CancellationTokenSource _cts = new();

   private bool _stopped;
   private bool _isDisposed;

   private readonly Lock _lock = new();

   public PipeReader Input => _readPipe.Reader;
   public PipeWriter Output => _writePipe.Writer;

   public Stream? InnerStream { get; private set; }

   public void Initialize(Stream stream)
   {
      ArgumentNullException.ThrowIfNull(stream);
      if (stream is { CanRead: false, CanWrite: false })
      {
         throw new ArgumentException("Stream must be readable or writable.", nameof(stream));
      }

      InnerStream = stream;
   }

   public void Start()
   {
      if (InnerStream == null)
      {
         throw new InvalidOperationException("StreamConnection must be initialized with a Stream before starting.");
      }

      lock (_lock)
      {
         if (_stopped)
         {
            throw new InvalidOperationException("Cannot start a stopped StreamConnection.");
         }

         if (InnerStream.CanWrite)
         {
            _writeTask = Task.Run(CopyWritePipeToStream);
         }
         else
         {
            _writePipe.Reader.Complete();
         }

         if (InnerStream.CanRead)
         {
            _readTask = Task.Run(CopyStreamToReadPipe);
         }
         else
         {
            _readPipe.Writer.Complete();
         }
      }
   }

   public void Stop()
   {
      lock (_lock)
      {
         if (_stopped)
         {
            _readPipe.Reader.Complete();
            _readPipe.Writer.Complete();

            _writePipe.Reader.Complete();
            _writePipe.Writer.Complete();

            return;
         }

         _stopped = true;
         _cts.Cancel();
      }

      _readPipe.Reader.Complete();
      _readPipe.Writer.Complete();

      _writePipe.Reader.Complete();
      _writePipe.Writer.Complete();
   }

   public async ValueTask StopAsync()
   {
      await _writePipe.Writer.CompleteAsync();

      lock (_lock)
      {
         if (!_stopped)
         {
            _stopped = true;
            _cts.Cancel();
         }
      }

      var stream = InnerStream;
      if (stream is not null)
      {
         try
         {
            if (stream.CanWrite)
            {
               using var flushCts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
               await stream.FlushAsync(flushCts.Token).ConfigureAwait(false);
            }
         }
         catch
         {
            // Expected
         }
         finally
         {
            try
            {
               await stream.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
               // Expected
            }
         }
      }

      Stop();

      var readTask = _readTask;
      if (readTask is not null)
      {
         try
         {
            await readTask;
         }
         catch
         {
            // Expected
         }
      }

      var writeTask = _writeTask;
      if (writeTask is not null)
      {
         try
         {
            await writeTask;
         }
         catch
         {
            // Expected
         }
      }
   }

   public async ValueTask DisposeAsync()
   {
      lock (_lock)
      {
         if (_isDisposed) return;
         _isDisposed = true;
      }

      await StopAsync();
      _cts.Dispose();
   }

   public bool TryResetState()
   {
      if ((_readTask is { IsCompleted: false })
          || (_writeTask is { IsCompleted: false }))
      {
         return false;
      }

      Stop();

      _readPipe.Reset();
      _writePipe.Reset();

      InnerStream = null;
      _stopped = false;
      _isDisposed = false;

      _cts.Dispose();
      _cts = new CancellationTokenSource();

      _readTask = null;
      _writeTask = null;

      return true;
   }

   private async Task CopyWritePipeToStream()
   {
      var stream = InnerStream;
      if (stream == null) return;

      var reader = _writePipe.Reader;

      try
      {
         while (true)
         {
            var pending = reader.ReadAsync(_cts.Token);

            if (!pending.IsCompleted)
            {
               await stream.FlushAsync(_cts.Token);
            }

            var result = await pending;
            ReadOnlySequence<byte> buffer;

            do
            {
               buffer = result.Buffer;

               if (!buffer.IsEmpty)
                  await SetBuffer(stream, buffer);

               reader.AdvanceTo(buffer.End);

            } while (!(buffer.IsEmpty && result.IsCompleted)
                        && reader.TryRead(out result));

            if (buffer.IsEmpty && result.IsCompleted)
               break;

            if (result.IsCanceled)
               break;
         }

         await reader.CompleteAsync();
      }
      catch (Exception er)
      {
         try
         {
            await reader.CompleteAsync(er);
         }
         catch
         {
            // Expected
         }
      }
   }

   private async Task CopyStreamToReadPipe()
   {
      var stream = InnerStream;
      if (stream == null) return;

      Exception? error = null;
      var writer = _readPipe.Writer;

      try
      {
         while (true)
         {
            var memory = writer.GetMemory(MinAllocBufferSize);
            var read = await stream.ReadAsync(memory, _cts.Token);

            if (read <= 0)
               break;

            TraceLogger.LogNeutralInfo("StreamConnection: Read {0} bytes from stream", read);

            writer.Advance(read);

            var fres = await writer.FlushAsync(_cts.Token);
            if (fres.IsCanceled || fres.IsCompleted)
               break;
         }
      }
      catch (Exception er)
      {
         error = er;
      }

      await writer.CompleteAsync(error);
   }

   private Task SetBuffer(Stream stream, in ReadOnlySequence<byte> data)
   {
      TraceLogger.LogNeutralInfo("StreamConnection: Transmitting {0} bytes to stream", data.Length);
      if (data.IsSingleSegment)
      {
         var vtask = stream.WriteAsync(data.First, _cts.Token);
         return vtask.IsCompletedSuccessfully ? Task.CompletedTask : vtask.AsTask();
      }

      return SetBufferSegments(stream, data);
   }

   private async Task SetBufferSegments(Stream stream, ReadOnlySequence<byte> data)
   {
      foreach (var segment in data)
      {
         await stream.WriteAsync(segment, _cts.Token);
      }
   }
}
