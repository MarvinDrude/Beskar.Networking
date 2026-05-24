using System.IO.Pipelines;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Transports.Common.Streams;
using Beskar.Networking.Transports.Common.Settings;
using Me.Memory.Pools;
using Me.Memory.Results;

namespace Beskar.Networking.Transports.Quic;

/// <summary>
/// Represents a QUIC network session that supports stream multiplexing.
/// </summary>
public sealed class QuicNetworkSession : INetworkSession, IAsyncDisposable
{
   public Guid Id { get; } = Guid.CreateVersion7();

   public EndPoint RemoteAddress => _connection.RemoteEndPoint;
   public EndPoint LocalAddress => _connection.LocalEndPoint;

   public bool IsSupportingMultiplexing => true;
   public bool IsSupportingUnidirectional => true;

   public CancellationToken SessionClosedToken => _cts.Token;

   private readonly QuicConnection _connection;
   private readonly QuicTransportOptions _options;
   private readonly CancellationTokenSource _cts = new();

   private readonly StreamQueueSettings _streamSettings;
   private readonly AsyncDisposableObjectPool<StreamConnection> _streamConnectionPool;

   private int _disposed;

   public QuicNetworkSession(QuicConnection connection, QuicTransportOptions options)
   {
      _connection = connection;
      _options = options;
      _streamSettings = options.StreamOptions.CreateQueueSettings();

      _streamConnectionPool = new AsyncDisposableObjectPool<StreamConnection>(new ObjectPoolOptions<StreamConnection>
      {
         FactoryFunc = static () => throw new InvalidOperationException("Use the parameterised Get overload to instantiate StreamConnections."),
         ReturnFunc = static connection => connection.TryResetState(),
         InitialSize = 0,
         MaxSize = options.StreamOptions.MaxConnectionPoolSize
      });
   }

   public async ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
   {
      try
      {
         using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
         var quicStream = await _connection.AcceptInboundStreamAsync(linkedCts.Token);

         var connection = _streamConnectionPool.Get(() => new StreamConnection(
            _streamSettings.ReceiveOptions, _streamSettings.SendOptions));

         connection.Initialize(quicStream);
         connection.Start();

         var newStream = new QuicNetworkStream(this, quicStream, connection);
         return newStream;
      }
      catch (QuicException ex)
      {
         return new NetworkCodeError((int)ex.QuicError, ex.Message);
      }
      catch (OperationCanceledException) when (_cts.IsCancellationRequested)
      {
         return new NetworkCodeError(-1, "Session has been closed.");
      }
      catch (Exception ex)
      {
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   public async ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
      CancellationToken ct = default)
   {
      try
      {
         using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
         var quicStreamType = direction == NetworkStreamDirection.Bidirectional
            ? QuicStreamType.Bidirectional
            : QuicStreamType.Unidirectional;

         var quicStream = await _connection.OpenOutboundStreamAsync(quicStreamType, linkedCts.Token);

         var connection = _streamConnectionPool.Get(() => new StreamConnection(
            _streamSettings.ReceiveOptions, _streamSettings.SendOptions));

         connection.Initialize(quicStream);
         connection.Start();

         return new QuicNetworkStream(this, quicStream, connection);
      }
      catch (QuicException ex)
      {
         return new NetworkCodeError((int)ex.QuicError, ex.Message);
      }
      catch (OperationCanceledException) when (_cts.IsCancellationRequested)
      {
         return new NetworkCodeError(-1, "Session has been closed.");
      }
      catch (Exception ex)
      {
         return new NetworkCodeError(-1, ex.Message);
      }
   }

   /// <summary>
   /// Returns a retired stream connection adapter to the shared connection pool.
   /// </summary>
   public async ValueTask ReturnConnectionAsync(StreamConnection connection)
   {
      await _streamConnectionPool.ReturnAsync(connection);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      try
      {
         await _cts.CancelAsync();
      }
      catch
      {
         // Ignored
      }
      _cts.Dispose();

      try
      {
         await _connection.CloseAsync(_options.DefaultCloseErrorCode);
      }
      catch
      {
         // Ignored
      }

      await _connection.DisposeAsync();
      await _streamConnectionPool.DisposeAsync();

      _streamSettings.Dispose();
   }
}
