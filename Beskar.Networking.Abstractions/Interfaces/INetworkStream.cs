using System.IO.Pipelines;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Networking.Abstractions.Interfaces;

/// <summary>
/// Represents a network stream.
/// </summary>
public interface INetworkStream : IAsyncDisposable
{
   /// <summary>
   /// The unique identifier of the network stream.
   /// </summary>
   public long StreamId { get; }

   /// <summary>
   /// The origin network session of the current network stream.
   /// </summary>
   public INetworkSession Session { get; }

   /// <summary>
   /// The direction of the network stream.
   /// </summary>
   public NetworkStreamDirection Direction { get; }

   /// <summary>
   /// The transport of the network stream.
   /// </summary>
   public IDuplexPipe Transport { get; }

   /// <summary>
   /// Acquires a lock to safely write to sending Transport.
   /// <remarks>MUST be disposed after done.</remarks>
   /// </summary>
   public ValueTask<LockReleaser> AcquireWriterLock(CancellationToken cancellationToken = default);
}
