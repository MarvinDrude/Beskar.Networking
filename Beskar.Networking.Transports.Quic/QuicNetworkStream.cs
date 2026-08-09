using System.IO.Pipelines;
using System.Net.Quic;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Telemetry;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Networking.Transports.Common.Streams;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Transports.Quic;

/// <summary>
/// Represents a single multiplexed QUIC network stream wrapping an underlying StreamConnection pipeline.
/// </summary>
public sealed class QuicNetworkStream : INetworkStream
{
   private readonly QuicNetworkSession _session;
   private readonly QuicStream _quicStream;
   private readonly StreamConnection _connection;
   private readonly AsyncLock _asyncLock = new();

   private int _disposed;
   public long StreamId => _quicStream.Id;

   public INetworkSession Session => _session;

   public IDuplexPipe Transport { get; }

   private long _bytesReceived;
   private long _bytesSent;
   private long _lastReceivedTimestampTicks;
   private long _lastSentTimestampTicks;

   public NetworkStats Stats
   {
      get => new()
      {
         BytesReceived = Volatile.Read(ref _bytesReceived),
         BytesSent = Volatile.Read(ref _bytesSent),
         LastReceivedTimestamp = _lastReceivedTimestampTicks == 0 ? null : new DateTimeOffset(_lastReceivedTimestampTicks, TimeSpan.Zero),
         LastSentTimestamp = _lastSentTimestampTicks == 0 ? null : new DateTimeOffset(_lastSentTimestampTicks, TimeSpan.Zero)
      };
      set
      {
         Volatile.Write(ref _bytesReceived, value.BytesReceived);
         Volatile.Write(ref _bytesSent, value.BytesSent);
         Volatile.Write(ref _lastReceivedTimestampTicks, value.LastReceivedTimestamp?.UtcTicks ?? 0);
         Volatile.Write(ref _lastSentTimestampTicks, value.LastSentTimestamp?.UtcTicks ?? 0);
      }
   }

   public void IncrementBytesReceived(long bytes)
   {
      Interlocked.Add(ref _bytesReceived, bytes);
      Volatile.Write(ref _lastReceivedTimestampTicks, DateTimeOffset.UtcNow.UtcTicks);
      TransportMetrics.RecordBytesReceived(bytes, Session.Transport);
   }

   public void IncrementBytesSent(long bytes)
   {
      Interlocked.Add(ref _bytesSent, bytes);
      Volatile.Write(ref _lastSentTimestampTicks, DateTimeOffset.UtcNow.UtcTicks);
      TransportMetrics.RecordBytesSent(bytes, Session.Transport);
   }

   public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;

   public QuicNetworkStream(QuicNetworkSession session, QuicStream quicStream, StreamConnection connection)
   {
      _session = session;
      _quicStream = quicStream;
      _connection = connection;

      Transport = new StatsTrackingDuplexPipe(connection, this);
      TransportMetrics.RecordStreamOpened(TransportKind.Quic);
   }

   public NetworkStreamDirection Direction => _quicStream.Type == QuicStreamType.Bidirectional
      ? NetworkStreamDirection.Bidirectional
      : NetworkStreamDirection.Unidirectional;

   public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default)
   {
      return _asyncLock.LockAsync(cancellationToken);
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return;
      }

      TransportMetrics.RecordStreamClosed(TransportKind.Quic);

      TraceLogger.LogNeutralInfo("QUIC Stream: Disposing stream wrapper {0} (Direction: {1}) for session {2}", StreamId, Direction, Session.Id);

      try
      {
         if (_quicStream.CanWrite)
         {
            _quicStream.CompleteWrites();
         }
      }
      catch
      {
         // Ignored
      }

      try
      {
         await _quicStream.DisposeAsync();
      }
      catch
      {
         // Ignored
      }

      await _session.ReturnConnectionAsync(this, _connection);
   }
}
