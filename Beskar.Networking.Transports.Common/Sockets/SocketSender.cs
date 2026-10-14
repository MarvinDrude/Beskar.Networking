using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces.Pools;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Transports.Common.Sockets;

public sealed class SocketSender(PipeOptions pipeOptions)
   : PipeWriter, IPooledObject, IAsyncDisposable
{
   public Pipe Pipe { get; } = new(pipeOptions);

   private SocketConnection? _connection;
   private Socket? _socket;

   private Task? _sendTask;
   private CancellationTokenSource _cts = new();
   private bool _stopped;
   private bool _isDisposed;

   public SocketSender(MemoryPool<byte> bufferPool)
      : this(new PipeOptions(
         pool: bufferPool,
         useSynchronizationContext: false))
   {
   }

   public void Initialize(SocketConnection connection, Socket socket)
   {
      _connection = connection;
      _socket = socket;
   }

   public void Start()
   {
      if (_socket == null)
      {
         throw new InvalidOperationException("SocketSender must be initialized with a Socket before starting.");
      }

      lock (_cts)
      {
         if (_stopped)
         {
            throw new InvalidOperationException("Cannot start a stopped SocketSender.");
         }

         _sendTask ??= Task.Run(ProcessSendAsync);
      }
   }

   private void EnsureStarted()
   {
      if (_sendTask != null || _socket == null) return;

      lock (_cts)
      {
         if (!_stopped && _sendTask == null && _socket != null)
         {
            _sendTask = Task.Run(ProcessSendAsync);
         }
      }
   }

   public void Stop()
   {
      lock (_cts)
      {
         if (_stopped) return;
         _stopped = true;

         _cts.Cancel();
      }

      if (_sendTask is null || _sendTask.IsCompleted)
      {
         Pipe.Reader.Complete();
      }

      Pipe.Writer.Complete();
   }

   public async ValueTask StopAsync()
   {
      await Pipe.Writer.CompleteAsync();

      var sendTask = _sendTask;
      if (sendTask is not null)
      {
         try
         {
            using var delayCts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await sendTask.WaitAsync(delayCts.Token);
         }
         catch
         {
            lock (_cts)
            {
               if (!_stopped)
               {
                  _stopped = true;
                  _cts.Cancel();
               }
            }
         }
      }

      Stop();

      if (sendTask is not null)
      {
         try
         {
            await sendTask;
         }
         catch
         {
            // expected
         }
      }
   }

   public override bool CanGetUnflushedBytes => Pipe.Writer.CanGetUnflushedBytes;

   public override long UnflushedBytes => Pipe.Writer.UnflushedBytes;

   public override void Advance(int bytes) => Pipe.Writer.Advance(bytes);

   public override Memory<byte> GetMemory(int sizeHint = 0) => Pipe.Writer.GetMemory(sizeHint);

   public override Span<byte> GetSpan(int sizeHint = 0) => Pipe.Writer.GetSpan(sizeHint);

   public override void CancelPendingFlush() => Pipe.Writer.CancelPendingFlush();

   public override void Complete(Exception? exception = null) => Pipe.Writer.Complete(exception);

   public override ValueTask CompleteAsync(Exception? exception = null) => Pipe.Writer.CompleteAsync(exception);

   public override ValueTask<FlushResult> FlushAsync(CancellationToken cancellationToken = default)
   {
      EnsureStarted();
      return Pipe.Writer.FlushAsync(cancellationToken);
   }

   private async Task ProcessSendAsync()
   {
      var socket = _socket;
      if (socket == null) return;

      try
      {
         while (true)
         {
            var result = await Pipe.Reader.ReadAsync(_cts.Token);
            var buffer = result.Buffer;

            if ((buffer.IsEmpty && result.IsCompleted) || result.IsCanceled)
            {
               break;
            }

            if (!buffer.IsEmpty)
            {
               await SendBufferAsync(socket, buffer, _cts.Token);
            }

            Pipe.Reader.AdvanceTo(buffer.End);

            if (result.IsCompleted)
            {
               break;
            }
         }
      }
      catch (OperationCanceledException)
      {
      }
      catch (Exception ex)
      {
         _connection?.Abort(ex);
      }
      finally
      {
         await Pipe.Reader.CompleteAsync();
      }
   }

   private static async ValueTask SendBufferAsync(Socket socket, ReadOnlySequence<byte> buffer, CancellationToken cancellationToken)
   {
      if (buffer.IsSingleSegment)
      {
         await SendMemoryAsync(socket, buffer.First, cancellationToken);
      }
      else
      {
         foreach (var memory in buffer)
         {
            await SendMemoryAsync(socket, memory, cancellationToken);
         }
      }
   }

   private static async ValueTask SendMemoryAsync(Socket socket, ReadOnlyMemory<byte> memory, CancellationToken cancellationToken)
   {
      while (!memory.IsEmpty)
      {
         var bytesSent = await socket.SendAsync(memory, SocketFlags.None, cancellationToken);

         if (bytesSent == 0)
         {
            throw new SocketException((int)SocketError.ConnectionAborted);
         }

         TraceLogger.LogNeutralInfo("SocketSender: Transmitted {0} bytes to socket", bytesSent);

         memory = memory[bytesSent..];
      }
   }

   public bool TryResetState()
   {
      if (_isDisposed)
      {
         return false;
      }

      if (_sendTask is { IsCompleted: false })
      {
         if (Pipe.Writer.UnflushedBytes > 0)
         {
            return false;
         }

         Stop();
         try
         {
            _sendTask.GetAwaiter().GetResult();
         }
         catch
         {
            // ignored
         }
      }

      lock (_cts)
      {
         _stopped = true;
         _cts.Cancel();
      }

      try
      {
         Pipe.Writer.Complete();

         while (Pipe.Reader.TryRead(out var result))
         {
            Pipe.Reader.AdvanceTo(result.Buffer.End);
            if (result.IsCompleted || result.Buffer.IsEmpty) break;
         }
      }
      catch (Exception) { /* ignored */ }

      Pipe.Reader.Complete();
      Pipe.Reset();

      _sendTask = null;
      _connection = null;
      _socket = null;
      _stopped = false;
      _isDisposed = false;

      _cts.Dispose();
      _cts = new CancellationTokenSource();

      return true;
   }

   public async ValueTask DisposeAsync()
   {
      if (_isDisposed) return;
      _isDisposed = true;

      await StopAsync();

      try
      {
         while (Pipe.Reader.TryRead(out var result))
         {
            Pipe.Reader.AdvanceTo(result.Buffer.End);
            if (result.IsCompleted || result.Buffer.IsEmpty) break;
         }
      }
      catch (Exception) { /* ignored */ }

      Pipe.Reader.Complete();
      _cts.Dispose();
   }
}
