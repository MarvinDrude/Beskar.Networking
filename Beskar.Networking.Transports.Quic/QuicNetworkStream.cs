using System.IO.Pipelines;
using System.Net.Quic;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
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

   public NetworkStats Stats { get; set; }

   public QuicNetworkStream(QuicNetworkSession session, QuicStream quicStream, StreamConnection connection)
   {
      _session = session;
      _quicStream = quicStream;
      _connection = connection;

      Transport = new StatsTrackingDuplexPipe(connection, this);
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

      TraceLogger.LogNeutralInfo("QUIC Stream: Disposing stream wrapper {0} (Direction: {1}) for session {2}", StreamId, Direction, Session.Id);

      await _connection.StopAsync();
      await _quicStream.DisposeAsync();

      await _session.ReturnConnectionAsync(this, _connection);
   }
}
