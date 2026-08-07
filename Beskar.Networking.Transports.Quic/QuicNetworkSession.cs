using System.Net;
using System.Net.Quic;
using System.Collections.Concurrent;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Telemetry;
using Beskar.Networking.Transports.Common.Streams;
using Beskar.Utilities.Tracing;
using Beskar.Memory.Results;

namespace Beskar.Networking.Transports.Quic;

/// <summary>
/// Represents a QUIC network session that supports stream multiplexing.
/// </summary>
public sealed class QuicNetworkSession : INetworkSession
{
   public Guid Id { get; } = Guid.CreateVersion7();

   public EndPoint RemoteAddress => _connection.RemoteEndPoint;
   public EndPoint LocalAddress => _connection.LocalEndPoint;

   public bool IsSupportingMultiplexing => true;
   public bool IsSupportingUnidirectional => true;

   public CancellationToken SessionClosedToken => _cts.Token;

   public INetworkPropertyStore Properties { get; } = new NetworkPropertyStore();

   public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;
   public TransportKind Transport => TransportKind.Quic;
   public QuicTransportOptions Options => _options;

   public NetworkSecurityInfo SecurityInfo => new(
      IsEncrypted: true,
      Protocol: _connection.SslProtocol,
      CipherSuite: _connection.NegotiatedCipherSuite.ToString(),
      LocalCertificate: null,
      RemoteCertificate: _connection.RemoteCertificate
   );

   private long _streamsAccepted;
   private long _streamsOpened;

   public NetworkSessionStats SessionStats => new()
   {
      StreamsAccepted = Interlocked.Read(ref _streamsAccepted),
      StreamsOpened = Interlocked.Read(ref _streamsOpened)
   };

   public IReadOnlyCollection<INetworkStream> ActiveStreams => [.. _activeStreams.Values];

   public NetworkStats Stats
   {
      get
      {
         var received = Interlocked.Read(ref _accumulatedBytesReceived);
         var sent = Interlocked.Read(ref _accumulatedBytesSent);

         DateTimeOffset? lastReceived;
         DateTimeOffset? lastSent;

         lock (_statsLock)
         {
            lastReceived = _accumulatedLastReceivedTimestamp;
            lastSent = _accumulatedLastSentTimestamp;
         }

         foreach (var stream in _activeStreams.Values)
         {
            var streamStats = stream.Stats;
            received += streamStats.BytesReceived;
            sent += streamStats.BytesSent;

            if (streamStats.LastReceivedTimestamp > lastReceived
                || (lastReceived is null && streamStats.LastReceivedTimestamp is not null))
            {
               lastReceived = streamStats.LastReceivedTimestamp;
            }
            if (streamStats.LastSentTimestamp > lastSent
                || (lastSent is null && streamStats.LastSentTimestamp is not null))
            {
               lastSent = streamStats.LastSentTimestamp;
            }
         }

         return new NetworkStats
         {
            BytesReceived = received,
            BytesSent = sent,
            LastReceivedTimestamp = lastReceived,
            LastSentTimestamp = lastSent
         };
      }
   }

   private readonly QuicConnection _connection;
   private readonly QuicTransportOptions _options;
   private readonly QuicIoQueueRegistry _ioQueueRegistry;
   private readonly CancellationTokenSource _cts = new();

   private readonly ConcurrentDictionary<long, QuicNetworkStream> _activeStreams = new();

   private int _disposed;

   private long _accumulatedBytesReceived;
   private long _accumulatedBytesSent;

   private readonly Lock _statsLock = new();
   private DateTimeOffset? _accumulatedLastReceivedTimestamp;
   private DateTimeOffset? _accumulatedLastSentTimestamp;

   public QuicNetworkSession(
      QuicConnection connection,
      QuicTransportOptions options,
      QuicIoQueueRegistry ioQueueRegistry)
   {
      _connection = connection;
      _options = options;
      _ioQueueRegistry = ioQueueRegistry;

      TransportMetrics.RecordConnectionOpened(TransportKind.Quic);
   }

   public async ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(CancellationToken ct = default)
   {
      QuicStream? quicStream = null;
      StreamConnection? connection = null;
      var success = false;

      try
      {
         TraceLogger.LogServerInfo("QUIC Session {0}: Accepting incoming stream...", Id);
         using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
         quicStream = await _connection.AcceptInboundStreamAsync(linkedCts.Token);

         connection = _ioQueueRegistry.Create(quicStream);

         var newStream = new QuicNetworkStream(this, quicStream, connection);
         _activeStreams.TryAdd(newStream.StreamId, newStream);
         Interlocked.Increment(ref _streamsAccepted);

         TraceLogger.LogServerInfo("QUIC Session {0}: Successfully accepted inbound {1} stream {2}", Id, newStream.Direction, newStream.StreamId);
         success = true;

         return newStream;
      }
      catch (QuicException ex)
      {
         if (ex.QuicError is QuicError.ConnectionAborted or QuicError.ConnectionTimeout or QuicError.OperationAborted)
         {
            try { await _cts.CancelAsync(); } catch { /* Ignored */ }
         }

         if (ex.QuicError == QuicError.ConnectionAborted)
         {
            TraceLogger.LogServerInfo("QUIC Session {0}: Connection closed by peer gracefully or aborted (Application Error Code: {1})", Id, ex.ApplicationErrorCode);
         }
         else
         {
            TraceLogger.LogServerError("QUIC Session {0}: QuicException accepting inbound stream (Code: {1}): {2}", Id, (int)ex.QuicError, ex.Message);
         }
         return new NetworkCodeError((int)ex.QuicError, ex.Message);
      }
      catch (OperationCanceledException) when (_cts.IsCancellationRequested)
      {
         TraceLogger.LogServerWarning("QUIC Session {0}: Stream acceptance cancelled because session is closing.", Id);
         return new NetworkCodeError(-1, "Session has been closed.");
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("QUIC Session {0}: Unexpected exception accepting inbound stream: {1}", Id, ex.Message);
         return new NetworkCodeError(-1, ex.Message);
      }
      finally
      {
         if (!success)
         {
            if (connection is not null)
            {
               await _ioQueueRegistry.ReturnAsync(connection);
            }
            else if (quicStream is not null)
            {
               await quicStream.DisposeAsync();
            }
         }
      }
   }

   public async ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
      CancellationToken ct = default)
   {
      QuicStream? quicStream = null;
      StreamConnection? connection = null;
      var success = false;

      try
      {
         TraceLogger.LogClientInfo("QUIC Session {0}: Opening outbound stream (Direction: {1})...", Id, direction);
         using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, _cts.Token);
         var quicStreamType = direction == NetworkStreamDirection.Bidirectional
            ? QuicStreamType.Bidirectional
            : QuicStreamType.Unidirectional;

         quicStream = await _connection.OpenOutboundStreamAsync(quicStreamType, linkedCts.Token);
         connection = _ioQueueRegistry.Create(quicStream);

         var newStream = new QuicNetworkStream(this, quicStream, connection);
         _activeStreams.TryAdd(newStream.StreamId, newStream);
         Interlocked.Increment(ref _streamsOpened);

         TraceLogger.LogClientInfo("QUIC Session {0}: Successfully opened outbound {1} stream {2}", Id, newStream.Direction, newStream.StreamId);
         success = true;
         return newStream;
      }
      catch (QuicException ex)
      {
         if (ex.QuicError is QuicError.ConnectionAborted or QuicError.ConnectionTimeout or QuicError.OperationAborted)
         {
            try { await _cts.CancelAsync(); } catch { /* Ignored */ }
         }

         TraceLogger.LogClientError("QUIC Session {0}: QuicException opening outbound stream (Code: {1}): {2}", Id, (int)ex.QuicError, ex.Message);
         return new NetworkCodeError((int)ex.QuicError, ex.Message);
      }
      catch (OperationCanceledException) when (_cts.IsCancellationRequested)
      {
         TraceLogger.LogClientWarning("QUIC Session {0}: Stream opening cancelled because session is closing.", Id);
         return new NetworkCodeError(-1, "Session has been closed.");
      }
      catch (Exception ex)
      {
         TraceLogger.LogClientError("QUIC Session {0}: Unexpected exception opening outbound stream: {1}", Id, ex.Message);
         return new NetworkCodeError(-1, ex.Message);
      }
      finally
      {
         if (!success)
         {
            if (connection is not null)
            {
               await _ioQueueRegistry.ReturnAsync(connection);
            }
            else if (quicStream is not null)
            {
               await quicStream.DisposeAsync();
            }
         }
      }
   }

   /// <summary>
   /// Returns a retired stream connection adapter to the shared connection pool.
   /// </summary>
   public async ValueTask ReturnConnectionAsync(QuicNetworkStream stream, StreamConnection connection)
   {
      var stats = stream.Stats;
      Interlocked.Add(ref _accumulatedBytesReceived, stats.BytesReceived);
      Interlocked.Add(ref _accumulatedBytesSent, stats.BytesSent);

      lock (_statsLock)
      {
         if (stats.LastReceivedTimestamp > _accumulatedLastReceivedTimestamp
             || (_accumulatedLastReceivedTimestamp is null && stats.LastReceivedTimestamp is not null))
         {
            _accumulatedLastReceivedTimestamp = stats.LastReceivedTimestamp;
         }
         if (stats.LastSentTimestamp > _accumulatedLastSentTimestamp
             || (_accumulatedLastSentTimestamp is null && stats.LastSentTimestamp is not null))
         {
            _accumulatedLastSentTimestamp = stats.LastSentTimestamp;
         }
      }

      _activeStreams.TryRemove(stream.StreamId, out _);

      TraceLogger.LogNeutralInfo("QUIC Session {0}: Returning stream connection to registry pool", Id);
      await _ioQueueRegistry.ReturnAsync(connection);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      TransportMetrics.RecordConnectionClosed(TransportKind.Quic);

      TraceLogger.LogNeutralInfo("QUIC Session: Disposing and shutting down active session {0} (Remote: {1}, Local: {2})", Id, RemoteAddress, LocalAddress);

      try
      {
         await _cts.CancelAsync();
      }
      catch
      {
         // Ignored
      }

      foreach (var stream in _activeStreams.Values)
      {
         try
         {
            await stream.DisposeAsync();
         }
         catch
         {
            // Ignored
         }
      }
      _activeStreams.Clear();

      try
      {
         await _connection.DisposeAsync();
      }
      catch (Exception ex)
      {
         TraceLogger.LogNeutralWarning("QUIC Session {0}: Error disposing connection: {1}", Id, ex.Message);
      }

      _cts.Dispose();
   }
}
