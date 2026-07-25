using System.IO.Pipelines;
using Beskar.Networking.Transports.Common.Streams;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Pools;

namespace Beskar.Networking.Transports.NamedPipes;

public sealed class NamedPipeIoQueueRegistry : IAsyncDisposable
{
   private readonly ulong _ioQueueCountLong;
   private readonly NamedPipeIoQueue[] _ioQueues;

   private ulong _currentIndex;
   private bool _isDisposed;

   private readonly AsyncDisposableObjectPool<StreamConnection> _streamConnectionPool;

   public NamedPipeIoQueueRegistry(NamedPipeTransportOptions options)
   {
      _ioQueueCountLong = (ulong)options.IoQueueCount;
      _ioQueues = new NamedPipeIoQueue[options.IoQueueCount];

      _streamConnectionPool = new AsyncDisposableObjectPool<StreamConnection>(new ObjectPoolOptions<StreamConnection>()
      {
         FactoryFunc = static () => throw new InvalidOperationException(),
         ReturnFunc = static connection => connection.TryResetState(),
         InitialSize = 0,
         MaxSize = options.StreamOptions.MaxConnectionPoolSize,
      });

      for (var e = 0; e < options.IoQueueCount; e++)
      {
         _ioQueues[e] = new NamedPipeIoQueue(options)
         {
            StreamConnectionPool = _streamConnectionPool
         };
      }
   }

   public IDuplexPipe Create(Stream stream)
   {
      TraceLogger.LogNeutralInfo("NamedPipe IO Registry: Creating duplex pipe connection for stream");
      var ioQueue = _ioQueues[Interlocked.Increment(ref _currentIndex) % _ioQueueCountLong];
      return ioQueue.Create(stream);
   }

   public async ValueTask ReturnAsync(IDuplexPipe connection)
   {
      if (connection is StreamConnection streamConn)
      {
         await streamConn.StopAsync();

         if (_isDisposed)
         {
            await streamConn.DisposeAsync();
            return;
         }

         TraceLogger.LogNeutralInfo("NamedPipe IO Registry: Returning stream connection to pool");
         await _streamConnectionPool.ReturnAsync(streamConn);
      }
   }

   public async ValueTask DisposeAsync()
   {
      if (_isDisposed) return;
      _isDisposed = true;

      foreach (var setting in _ioQueues)
      {
         setting.Dispose();
      }

      await _streamConnectionPool.DisposeAsync();
   }
}
