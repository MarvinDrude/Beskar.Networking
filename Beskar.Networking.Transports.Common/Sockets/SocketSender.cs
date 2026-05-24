using System.Buffers;
using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces.Pools;

namespace Beskar.Networking.Transports.Common.Sockets;

/// <summary>
/// Represents a sender for a socket.
/// </summary>
public sealed class SocketSender(
   SocketConnection connection,
   Socket socket,
   PipeOptions pipeOptions)
   : IPooledObject, IAsyncDisposable
{
   /// <summary>
   /// The pipe used to send data.
   /// </summary>
   public Pipe Pipe { get; } = new(pipeOptions);

   private readonly SocketConnection _connection = connection;
   private readonly Socket _socket = socket;
   
   private Task? _sendTask;
   private CancellationTokenSource _cts = new();
   private bool _stopped;

   public void Start()
   {
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

      Pipe.Reader.Complete();
      Pipe.Writer.Complete();
   }

   private async Task ProcessSendAsync()
   {
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
               await SendBufferAsync(buffer, _cts.Token);
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
         // Expected when the connection is closed.
      }
      catch (Exception ex)
      {
         // Todo: notify connection of error
      }
      finally
      {
         await Pipe.Reader.CompleteAsync();
         await Pipe.Writer.CompleteAsync();
      }
   }

   private async ValueTask SendBufferAsync(ReadOnlySequence<byte> buffer, CancellationToken cancellationToken)
   {
      if (buffer.IsSingleSegment)
      {
         await SendMemoryAsync(buffer.First, cancellationToken);
      }
      else
      {
         foreach (var memory in buffer)
         {
            await SendMemoryAsync(memory, cancellationToken);
         }
      }
   }

   private async ValueTask SendMemoryAsync(ReadOnlyMemory<byte> memory, CancellationToken cancellationToken)
   {
      while (!memory.IsEmpty)
      {
         var bytesSent = await _socket.SendAsync(memory, SocketFlags.None, cancellationToken);
         
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

      _stopped = false;
      
      _cts.Dispose();
      _cts = new CancellationTokenSource();
      
      return true;
   }

   public async ValueTask DisposeAsync()
   {
      try
      {
         await Pipe.Writer.CompleteAsync();
         await Pipe.Reader.CompleteAsync();
      }
      catch (Exception)
      {
         // ignored
      }

      try
      {
         if (_sendTask is not null)
         {
            await _sendTask;
         }
      }
      catch (Exception)
      {
         // ignored
      }
   }
}