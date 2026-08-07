using System.IO.Pipelines;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Telemetry;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Transports.Udp;

/// <summary>
/// Represents a UDP network stream wrapper around an <see cref="IDuplexPipe"/>.
/// </summary>
public sealed class UdpNetworkStream : INetworkStream
{
   public long StreamId => 0;

   public INetworkSession Session { get; }
   public IDuplexPipe Transport { get; }

   public NetworkStreamDirection Direction => NetworkStreamDirection.Bidirectional;

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

   private int _disposed;

   public UdpNetworkStream(INetworkSession session, IDuplexPipe transport)
   {
      Session = session;
      Transport = new StatsTrackingDuplexPipe(transport, this);
      TransportMetrics.RecordStreamOpened(session.Transport);
   }

   private readonly AsyncLock _asyncLock = new();

   public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default)
   {
      return _asyncLock.LockAsync(cancellationToken);
   }

   public ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposed, 1) == 1)
      {
         return ValueTask.CompletedTask;
      }

      TransportMetrics.RecordStreamClosed(Session.Transport);
      TraceLogger.LogNeutralInfo("UDP Stream: Disposing stream wrapper for session {0}", Session.Id);
      return ValueTask.CompletedTask;
   }
}
