using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces.Pools;

namespace Beskar.Networking.Transports.Common.Sockets;

/// <summary>
/// Represents a sender for a socket.
/// </summary>
public sealed class SocketSender : IPooledObject, IAsyncDisposable
{
   /// <summary>
   /// The pipe used to send data.
   /// </summary>
   public Pipe Pipe { get; }
   
   private SocketConnection? _connection;
   private Socket? _socket;
   
   private Task? _sendTask;
   private CancellationTokenSource _cts = new();
   private bool _stopped;

   /// <summary>
   /// Initializes a new instance of the <see cref="SocketSender"/> class with pool-level invariants.
   /// </summary>
   public SocketSender(PipeOptions pipeOptions)
   {
      Pipe = new Pipe(pipeOptions);
   }

   /// <summary>
   /// Initializes the sender for a rented session.
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
         throw new InvalidOperationException("SocketSender must be initialized with a Socket and SocketConnection before starting.");
      }

      lock (_cts)
      {
         if (_stopped)
         {
            throw new InvalidOperationException("Cannot start a stopped SocketSender.");
         }
         
         _sendTask = Task.Run(ProcessSendAsync);
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

      // Complete the reader and writer to unblock any pending I/O operations.
      Pipe.Reader.Complete();
      Pipe.Writer.Complete();
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
         // Suppress cancellation exceptions when stopping.
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

   private async ValueTask SendBufferAsync(Socket socket, ReadOnlySequence<byte> buffer, CancellationToken cancellationToken)
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

   private async ValueTask SendMemoryAsync(Socket socket, ReadOnlyMemory<byte> memory, CancellationToken cancellationToken)
   {
      while (!memory.IsEmpty)
      {
         var bytesSent = await socket.SendAsync(memory, SocketFlags.None, cancellationToken);
         
         if (bytesSent == 0)
         {
            throw new SocketException((int)SocketError.ConnectionAborted);
         }
         
         memory = memory[bytesSent..];
      }
   }

   public bool TryResetState()
   {
      if (_sendTask is { IsCompleted: false })
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

      if (_sendTask is not null)
      {
         try
         {
            await _sendTask;
         }
         catch
         {
            // Suppress exception during disposal.
         }
      }

      _cts.Dispose();
   }
}