using System.IO.Pipelines;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Transports.Tcp;

/// <summary>
/// Represents a TCP network stream wrapper around an <see cref="IDuplexPipe"/>.
/// </summary>
public sealed class TcpNetworkStream : INetworkStream
{
   public long StreamId => 0;

   public INetworkSession Session { get; }
   public IDuplexPipe Transport { get; }

   public NetworkStreamDirection Direction => NetworkStreamDirection.Bidirectional;

   public NetworkStats Stats { get; set; }

   public DateTimeOffset CreatedAt { get; } = DateTimeOffset.UtcNow;

   public TcpNetworkStream(INetworkSession session, IDuplexPipe transport)
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
      TraceLogger.LogNeutralInfo("TCP Stream: Disposing stream wrapper for session {0}", Session.Id);
      return ValueTask.CompletedTask;
   }
}
