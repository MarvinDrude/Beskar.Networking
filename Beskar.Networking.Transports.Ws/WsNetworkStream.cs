using System.IO.Pipelines;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Transports.Ws;

/// <summary>
/// Represents a WebSocket network stream wrapping a <see cref="WsDuplexPipe"/>.
/// </summary>
public sealed class WsNetworkStream : INetworkStream
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
   }

   public void IncrementBytesSent(long bytes)
   {
      Interlocked.Add(ref _bytesSent, bytes);
      Volatile.Write(ref _lastSentTimestampTicks, DateTimeOffset.UtcNow.UtcTicks);
   }

   public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;

   public WsNetworkStream(INetworkSession session, IDuplexPipe transport)
   {
      Session = session;
      Transport = new StatsTrackingDuplexPipe(transport, this);
   }

   private readonly AsyncLock _asyncLock = new();

   public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default)
   {
      return _asyncLock.LockAsync(cancellationToken);
   }

   public ValueTask DisposeAsync()
   {
      TraceLogger.LogNeutralInfo("WS Stream: Disposing stream wrapper for session {0}", Session.Id);
      return ValueTask.CompletedTask;
   }
}
