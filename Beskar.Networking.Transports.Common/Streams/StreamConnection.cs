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

   private Action? _connectionClosedHandler;
   private int _connectionClosed;

   public event Action? ConnectionClosed
   {
      add
      {
         _connectionClosedHandler += value;
         if (Volatile.Read(ref _connectionClosed) == 1)
         {
            try
            {
               value?.Invoke();
            }
            catch
            {
               // Ignored
            }
         }
      }
      remove => _connectionClosedHandler -= value;
   }

   public PipeReader Input => _readPipe.Reader;
   public PipeWriter Output => _writePipe.Writer;

   public Stream? InnerStream { get; private set; }
   private bool AllowFlushOnStop { get; set; } = true;

   public void Initialize(Stream stream, bool allowFlushOnStop = true)
   {
      ArgumentNullException.ThrowIfNull(stream);
      if (stream is { CanRead: false, CanWrite: false })
      {
         throw new ArgumentException("Stream must be readable or writable.", nameof(stream));
      }

      Volatile.Write(ref _connectionClosed, 0);
      InnerStream = stream;
      AllowFlushOnStop = allowFlushOnStop;
   }

   internal void NotifyConnectionClosed()
   {
      if (Interlocked.Exchange(ref _connectionClosed, 1) == 1)
      {
         return;
      }

      try
      {
         _connectionClosedHandler?.Invoke();
      }
      catch
      {
         // Ignored
      }
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

   public async ValueTask DrainWritesAsync()
   {
      await _writePipe.Writer.CompleteAsync();

      var writeTask = _writeTask;
      if (writeTask is not null)
      {
         try
         {
            using var drainCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await writeTask.WaitAsync(drainCts.Token).ConfigureAwait(false);
         }
         catch
         {
            // Expected on timeout or error
         }
      }

      var stream = InnerStream;
      if (stream is not null)
      {
         try
         {
            if (stream.CanWrite && AllowFlushOnStop && !_cts.IsCancellationRequested)
            {
               using var flushCts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));
               await stream.FlushAsync(flushCts.Token).ConfigureAwait(false);
            }
         }
         catch
         {
            // Expected
         }
      }
   }

   public async ValueTask StopAsync()
   {
      await DrainWritesAsync();
      var writeTask = _writeTask;
      var stream = InnerStream;

      lock (_lock)
      {
         if (!_stopped)
         {
            _stopped = true;
            _cts.Cancel();
         }
      }

      if (stream is not null)
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

      lock (_lock)
      {
         _stopped = true;
         _cts.Cancel();
      }

      try
      {
         _readPipe.Writer.Complete();
         while (_readPipe.Reader.TryRead(out var rResult))
         {
            _readPipe.Reader.AdvanceTo(rResult.Buffer.End);
            if (rResult.IsCompleted || rResult.Buffer.IsEmpty) break;
         }
      }
      catch { /* ignored */ }

      try
      {
         _writePipe.Writer.Complete();
         while (_writePipe.Reader.TryRead(out var wResult))
         {
            _writePipe.Reader.AdvanceTo(wResult.Buffer.End);
            if (wResult.IsCompleted || wResult.Buffer.IsEmpty) break;
         }
      }
      catch { /* ignored */ }

      _readPipe.Reader.Complete();
      _writePipe.Reader.Complete();

      _readPipe.Reset();
      _writePipe.Reset();

      _connectionClosedHandler = null;
      Volatile.Write(ref _connectionClosed, 0);

      InnerStream = null;
      AllowFlushOnStop = true;
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
            {
               NotifyConnectionClosed();
               break;
            }

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
         if (!_cts.IsCancellationRequested)
         {
            NotifyConnectionClosed();
         }
      }

      await writer.CompleteAsync(error);
   }

   private ValueTask SetBuffer(Stream stream, in ReadOnlySequence<byte> data)
   {
      TraceLogger.LogNeutralInfo("StreamConnection: Transmitting {0} bytes to stream", data.Length);
      if (data.IsSingleSegment)
      {
         return stream.WriteAsync(data.First, _cts.Token);
      }

      return SetBufferSegments(stream, data);
   }

   private async ValueTask SetBufferSegments(Stream stream, ReadOnlySequence<byte> data)
   {
      foreach (var segment in data)
      {
         await stream.WriteAsync(segment, _cts.Token);
      }
   }
}
