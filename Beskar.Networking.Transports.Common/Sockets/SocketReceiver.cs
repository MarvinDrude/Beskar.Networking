using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces.Pools;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Pools;
using Beskar.Networking.Transports.Common.Pipelines;

namespace Beskar.Networking.Transports.Common.Sockets;

public sealed class SocketReceiver(PipeOptions pipeOptions)
   : IPooledObject, IAsyncDisposable
{
   private const int MinAllocBufferSize = 512;

   public Pipe Pipe { get; private set; } = new(pipeOptions);

   private SocketConnection? _connection;
   private Socket? _socket;

   private Task? _receiveTask;
   private CancellationTokenSource _cts = new();
   private bool _stopped;
   private bool _isDisposed;

   public void Initialize(SocketConnection connection, Socket socket)
   {
      _connection = connection;
      _socket = socket;
   }

   public void Start()
   {
      if (_socket == null || _connection == null)
      {
         throw new InvalidOperationException("SocketReceiver must be initialized with a Socket and SocketConnection before starting.");
      }

      lock (_cts)
      {
         if (_stopped)
         {
            throw new InvalidOperationException("Cannot start a stopped SocketReceiver.");
         }

         _receiveTask = Task.Run(ProcessReceiveAsync);
      }
   }

   public void Stop()
   {
      lock (_cts)
      {
         if (_stopped) return;
         _stopped = true;

         if (!_isDisposed)
         {
            _cts.Cancel();
         }
      }

      if (_receiveTask is null || _receiveTask.IsCompleted)
      {
         Pipe.Writer.Complete();
      }
      else
      {
         Pipe.Writer.CancelPendingFlush();
      }
   }

   public async ValueTask StopAsync()
   {
      Stop();

      if (_receiveTask is not null
          && Task.CurrentId != _receiveTask.Id)
      {
         try
         {
            await _receiveTask;
         }
         catch
         {
            // Expected
         }
      }
   }

   private async Task ProcessReceiveAsync()
   {
      var socket = _socket;
      if (socket == null) return;

      try
      {
         while (true)
         {
            var memory = Pipe.Writer.GetMemory(MinAllocBufferSize);

            var bytesRead = await socket.ReceiveAsync(memory, SocketFlags.None, _cts.Token);
            if (bytesRead == 0)
            {
               _connection?.NotifyConnectionClosed();
               break;
            }

            TraceLogger.LogNeutralInfo("SocketReceiver: Received {0} bytes from socket", bytesRead);

            Pipe.Writer.Advance(bytesRead);

            var result = await Pipe.Writer.FlushAsync(_cts.Token);
            if (result.IsCompleted || result.IsCanceled)
            {
               break;
            }
         }
      }
      catch (OperationCanceledException)
      {
         // Expected
      }
      catch (SocketException ex) when (
         ex.SocketErrorCode == SocketError.OperationAborted
         || _cts.IsCancellationRequested)
      {
         // Expected on socket cancellation
      }
      catch (Exception ex)
      {
         _connection?.Abort(ex);
      }
      finally
      {
         await Pipe.Writer.CompleteAsync();
      }
   }

   public bool TryResetState()
   {
      if (_isDisposed || _receiveTask is { IsCompleted: false })
      {
         return false;
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

      _receiveTask = null;
      _connection = null;
      _socket = null;

      _stopped = false;

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
         Pipe.Writer.Complete();

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
