using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces.Pools;
using Beskar.Networking.Transports.Common.Pipelines;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Transports.Common.Sockets;

public sealed class SocketSender : PipeWriter, IPooledObject, IAsyncDisposable
{
   private readonly MemoryPool<byte> _bufferPool;
   private SocketConnection? _connection;
   private Socket? _socket;

   private IMemoryOwner<byte>? _primaryBlock;
   private Memory<byte> _currentBuffer;
   private int _bytesWritten;

   private List<IMemoryOwner<byte>>? _extraBlocks;
   private List<ReadOnlyMemory<byte>>? _extraSegments;

   private bool _stopped;
   private bool _isCompleted;
   private bool _isCanceled;

   public SocketSender(MemoryPool<byte> bufferPool)
   {
      _bufferPool = bufferPool;
      EnsurePrimaryBlock();
   }

   public SocketSender(PipeOptions pipeOptions)
      : this(pipeOptions.Pool)
   {
   }

   private IMemoryOwner<byte> RentBlock(int size)
   {
      if (size <= _bufferPool.MaxBufferSize)
      {
         return _bufferPool.Rent(size);
      }

      var array = ArrayPool<byte>.Shared.Rent(size);
      return new ArrayPoolOwner(array);
   }

   private sealed class ArrayPoolOwner(byte[] array) : IMemoryOwner<byte>
   {
      public Memory<byte> Memory => array;
      public void Dispose() => ArrayPool<byte>.Shared.Return(array);
   }

   private void EnsurePrimaryBlock()
   {
      if (_primaryBlock == null)
      {
         _primaryBlock = RentBlock(NetworkPinnedBlockMemoryPool.BlockSize);

         _currentBuffer = _primaryBlock.Memory;
         _bytesWritten = 0;
      }
   }

   public void Initialize(SocketConnection connection, Socket socket)
   {
      _connection = connection;
      _socket = socket;

      _stopped = false;
      _isCompleted = false;
      _isCanceled = false;

      EnsurePrimaryBlock();
   }

   public void Start()
   {
      _stopped = false;
   }

   public void Stop()
   {
      _stopped = true;
      _isCompleted = true;
   }

   public ValueTask StopAsync()
   {
      Stop();
      return ValueTask.CompletedTask;
   }

   public override bool CanGetUnflushedBytes => true;

   public override long UnflushedBytes
   {
      get
      {
         long total = _bytesWritten;
         if (_extraSegments is { Count: > 0 })
         {
            foreach (var seg in _extraSegments)
            {
               total += seg.Length;
            }
         }

         return total;
      }
   }

   public override void Advance(int bytes)
   {
      if (bytes < 0)
      {
         throw new ArgumentOutOfRangeException(
            nameof(bytes), "Bytes to advance cannot be negative.");
      }

      if (_bytesWritten + bytes > _currentBuffer.Length)
      {
         throw new InvalidOperationException(
            "Cannot advance past the end of the buffer.");
      }

      _bytesWritten += bytes;
   }

   public override Memory<byte> GetMemory(int sizeHint = 0)
   {
      EnsurePrimaryBlock();
      if (sizeHint <= 0) sizeHint = 1;

      var remaining = _currentBuffer.Length - _bytesWritten;
      if (remaining >= sizeHint)
      {
         return _currentBuffer.Slice(_bytesWritten);
      }

      if (_bytesWritten == 0)
      {
         _primaryBlock?.Dispose();
         _primaryBlock = RentBlock(Math.Max(NetworkPinnedBlockMemoryPool.BlockSize, sizeHint));
         _currentBuffer = _primaryBlock.Memory;

         return _currentBuffer;
      }

      _extraBlocks ??= [];
      _extraSegments ??= [];

      _extraSegments.Add(_currentBuffer[.._bytesWritten]);
      _extraBlocks.Add(_primaryBlock!);

      _primaryBlock = RentBlock(Math.Max(NetworkPinnedBlockMemoryPool.BlockSize, sizeHint));
      _currentBuffer = _primaryBlock.Memory;
      _bytesWritten = 0;

      return _currentBuffer;
   }

   public override Span<byte> GetSpan(int sizeHint = 0)
   {
      return GetMemory(sizeHint).Span;
   }

   public override void CancelPendingFlush()
   {
      _isCanceled = true;
   }

   public override void Complete(Exception? exception = null)
   {
      _isCompleted = true;
   }

   public override ValueTask CompleteAsync(Exception? exception = null)
   {
      _isCompleted = true;
      return ValueTask.CompletedTask;
   }

   public override async ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
   {
      if (Volatile.Read(ref _isCanceled))
      {
         _isCanceled = false;
         return new FlushResult(isCanceled: true, isCompleted: _isCompleted);
      }

      var socket = _socket;
      if (socket == null || _isCompleted || _stopped)
      {
         return new FlushResult(isCanceled: false, isCompleted: true);
      }

      try
      {
         if (_extraSegments == null || _extraSegments.Count == 0)
         {
            if (_bytesWritten > 0)
            {
               var toSend = _currentBuffer.Slice(0, _bytesWritten);
               _bytesWritten = 0;
               await SendMemoryDirectAsync(socket, toSend, cancellationToken);
            }
         }
         else
         {
            if (_bytesWritten > 0)
            {
               _extraSegments.Add(_currentBuffer.Slice(0, _bytesWritten));
               _bytesWritten = 0;
            }

            try
            {
               foreach (var seg in _extraSegments)
               {
                  await SendMemoryDirectAsync(socket, seg, cancellationToken);
               }
            }
            finally
            {
               _extraSegments.Clear();
               if (_extraBlocks is { Count: > 0 })
               {
                  foreach (var block in _extraBlocks)
                  {
                     block.Dispose();
                  }
                  _extraBlocks.Clear();
               }
            }
         }
      }
      catch (OperationCanceledException)
      {
         return new FlushResult(isCanceled: true, isCompleted: _isCompleted);
      }
      catch (Exception ex)
      {
         _connection?.Abort(ex);
         throw;
      }

      return new FlushResult(isCanceled: false, isCompleted: _isCompleted);
   }

   private static async ValueTask SendMemoryDirectAsync(Socket socket, ReadOnlyMemory<byte> memory, CancellationToken cancellationToken)
   {
      while (!memory.IsEmpty)
      {
         var bytesSent = await socket.SendAsync(memory, SocketFlags.None, cancellationToken);
         if (bytesSent == 0)
         {
            throw new SocketException((int)SocketError.ConnectionAborted);
         }

         TraceLogger.LogNeutralInfo("SocketSender: Transmitted {0} bytes to socket", bytesSent);
         memory = memory.Slice(bytesSent);
      }
   }

   public bool TryResetState()
   {
      _bytesWritten = 0;

      _isCompleted = false;
      _isCanceled = false;
      _stopped = false;

      _connection = null;
      _socket = null;

      if (_extraSegments is { Count: > 0 })
      {
         _extraSegments.Clear();
      }

      if (_extraBlocks is { Count: > 0 })
      {
         foreach (var block in _extraBlocks)
         {
            block.Dispose();
         }
         _extraBlocks.Clear();
      }

      if (_primaryBlock != null && _currentBuffer.Length > 65536)
      {
         _primaryBlock.Dispose();

         _primaryBlock = null;
         EnsurePrimaryBlock();
      }

      return true;
   }

   public ValueTask DisposeAsync()
   {
      Stop();

      _primaryBlock?.Dispose();
      _primaryBlock = null;

      if (_extraBlocks is { Count: > 0 })
      {
         foreach (var block in _extraBlocks)
         {
            block.Dispose();
         }
         _extraBlocks.Clear();
      }

      return ValueTask.CompletedTask;
   }
}
