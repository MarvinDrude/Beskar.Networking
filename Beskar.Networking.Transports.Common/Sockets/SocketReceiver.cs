using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces.Pools;
using Me.Memory.Pools;

namespace Beskar.Networking.Transports.Common.Sockets;

/// <summary>
/// Represents a receiver for a socket.
/// </summary>
public sealed class SocketReceiver 
   : IPooledObject, IAsyncDisposable
{
   private static readonly int MinAllocBufferSize = PinnedBlockMemoryPool.BlockSize / 2;
   
   /// <summary>
   /// The pipe used to receive data.
   /// </summary>
   public Pipe Pipe { get; }
   
   private SocketConnection? _connection;
   private Socket? _socket;
   
   private Task? _receiveTask;
   private CancellationTokenSource _cts = new();
   private bool _stopped;

   /// <summary>
   /// Initializes a new instance of the <see cref="SocketReceiver"/> class with pool-level invariants.
   /// </summary>
   public SocketReceiver(PipeOptions pipeOptions)
   {
      Pipe = new Pipe(pipeOptions);
   }

   /// <summary>
   /// Initializes the receiver for a rented session.
   /// </summary>
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
         // Expected to happen
      }
      catch (Exception ex)
      {
         // notify
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
      Stop();

      if (_receiveTask is not null)
      {
         try
         {
            await _receiveTask;
         }
         catch
         {
            // Suppress exception during disposal.
         }
      }

      _cts.Dispose();
   }
}