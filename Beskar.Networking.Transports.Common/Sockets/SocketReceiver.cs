using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces.Pools;
using Me.Memory.Pools;

namespace Beskar.Networking.Transports.Common.Sockets;

public sealed class SocketReceiver(PipeOptions pipeOptions) 
   : IPooledObject, IAsyncDisposable
{
   private static readonly int MinAllocBufferSize = PinnedBlockMemoryPool.BlockSize / 2;
   
   public Pipe Pipe { get; } = new(pipeOptions);

   private SocketConnection? _connection;
   private Socket? _socket;
   
   private Task? _receiveTask;
   private CancellationTokenSource _cts = new();
   private bool _stopped;

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
         
         _cts.Cancel();
      }

      Pipe.Writer.Complete();
      Pipe.Reader.Complete();
   }

   public async ValueTask StopAsync()
   {
      Stop();

      if (_receiveTask is not null)
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
               break;
            }

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
      if (_receiveTask is { IsCompleted: false })
      {
         return false;
      }

      Pipe.Reset();

      _connection = null;
      _socket = null;
      
      _stopped = false;
      
      _cts.Dispose();
      _cts = new CancellationTokenSource();
      
      return true;
   }

   public async ValueTask DisposeAsync()
   {
      await StopAsync();
      _cts.Dispose();
   }
}