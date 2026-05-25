using System.IO.Pipelines;
using System.Net.Quic;
using Beskar.Networking.Transports.Common.Streams;
using Beskar.Networking.Transports.Common.Settings;
using Beskar.Utilities.Tracing;
using Me.Memory.Pools;

namespace Beskar.Networking.Transports.Quic;

/// <summary>
/// A registry that manages thread-safe sharing of StreamConnection object pools and capped memory allocations
/// across multiple QUIC sessions.
/// </summary>
public sealed class QuicIoQueueRegistry : IAsyncDisposable
{
   private readonly ulong _ioQueueCountLong;
   private readonly StreamQueueSettings[] _queueSettings;

   private readonly AsyncDisposableObjectPool<StreamConnection> _streamConnectionPool;

   private ulong _currentIndex;
   private bool _isDisposed;

   public QuicIoQueueRegistry(QuicTransportOptions options)
   {
      _ioQueueCountLong = (ulong)options.StreamOptions.IoQueueCount;
      _queueSettings = new StreamQueueSettings[_ioQueueCountLong];

      _streamConnectionPool = new AsyncDisposableObjectPool<StreamConnection>(new ObjectPoolOptions<StreamConnection>
      {
         FactoryFunc = static () => throw new InvalidOperationException("Use the parameterised Get overload to instantiate StreamConnections."),
         ReturnFunc = static connection => connection.TryResetState(),
         InitialSize = 0,
         MaxSize = options.StreamOptions.MaxConnectionPoolSize
      });

      for (var e = 0; e < options.StreamOptions.IoQueueCount; e++)
      {
         _queueSettings[e] = options.StreamOptions.CreateQueueSettings();
      }
   }

   public StreamConnection Create(Stream stream)
   {
      ObjectDisposedException.ThrowIf(_isDisposed, this);

      var ioQueueIndex = Interlocked.Increment(ref _currentIndex) % _ioQueueCountLong;
      var settings = _queueSettings[ioQueueIndex];

      var connection = _streamConnectionPool.Get(() => new StreamConnection(
         settings.ReceiveOptions, settings.SendOptions));

      if (stream is QuicStream quicStream)
      {
         TraceLogger.LogNeutralInfo("QUIC IO Registry: Creating stream connection for QUIC stream {0}", quicStream.Id);
      }
      else
      {
         TraceLogger.LogNeutralInfo("QUIC IO Registry: Creating stream connection for generic Stream");
      }

      connection.Initialize(stream);
      connection.Start();

      return connection;
   }

   public async ValueTask ReturnAsync(StreamConnection connection)
   {
      TraceLogger.LogNeutralInfo("QUIC IO Registry: Returning stream connection to pool");
      await _streamConnectionPool.ReturnAsync(connection);
   }

   public async ValueTask DisposeAsync()
   {
      if (_isDisposed)
      {
         return;
      }
      _isDisposed = true;

      foreach (var settings in _queueSettings)
      {
         settings.Dispose();
      }

      await _streamConnectionPool.DisposeAsync();
   }
}
