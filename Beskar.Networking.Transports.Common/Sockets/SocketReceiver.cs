using System.IO.Pipelines;
using System.Net.Sockets;
using Beskar.Networking.Abstractions.Interfaces.Pools;
using Me.Memory.Pools;

namespace Beskar.Networking.Transports.Common.Sockets;

/// <summary>
/// Represents a receiver for a socket.
/// </summary>
public sealed class SocketReceiver(
   SocketConnection connection,
   Socket socket,
   PipeOptions pipeOptions)
   : IPooledObject, IAsyncDisposable
{
   private static readonly int MinAllocBufferSize = PinnedBlockMemoryPool.BlockSize / 2;
   
   /// <summary>
   /// The pipe used to receive data.
   /// </summary>
   public Pipe Pipe { get; } = new(pipeOptions);
   
   private readonly SocketConnection _connection = connection;
   private readonly Socket _socket = socket;
   
   private Task? _receiveTask;
   private CancellationTokenSource _cts = new();
   private bool _stopped;
   
   
}