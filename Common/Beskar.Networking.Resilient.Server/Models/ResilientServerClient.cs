using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Protocol;

namespace Beskar.Networking.Resilient.Server.Models;

/// <summary>
/// Represents a connected client in the resilient server.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientServerClient<TFrame>(NetworkServerStreamContext streamContext)
   : IAsyncDisposable
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// Unique identifier for this connected client.
   /// </summary>
   public Guid Id { get; } = Guid.NewGuid();

   /// <summary>
   /// The network stream context for this client.
   /// </summary>
   public NetworkServerStreamContext StreamContext { get; } = streamContext;

   /// <summary>
   /// UTC timestamp when the client connected.
   /// </summary>
   public DateTimeOffset ConnectedAt { get; } = DateTimeOffset.UtcNow;

   /// <summary>
   /// UTC timestamp of the last activity (packet received or sent).
   /// </summary>
   public DateTimeOffset LastActivityAt => _lastActivityAt;

   /// <summary>
   /// Indicates whether the underlying transport stream session is active.
   /// </summary>
   public bool IsConnected => !StreamContext.Stream.Session.SessionClosedToken.IsCancellationRequested;

   private DateTimeOffset _lastActivityAt = DateTimeOffset.UtcNow;
   private int _disposedState;

   /// <summary>
   /// Updates the last activity timestamp to the current UTC time.
   /// </summary>
   public void TouchActivity()
   {
      _lastActivityAt = DateTimeOffset.UtcNow;
   }

   /// <summary>
   /// Asynchronously sends a frame to the client.
   /// </summary>
   public async ValueTask SendAsync(TFrame frame, CancellationToken cancellationToken = default)
   {
      if (Volatile.Read(ref _disposedState) == 1 || !IsConnected)
         return;

      using var writeLock = await StreamContext.Stream.AcquireWriterLock(cancellationToken);

      var pipeWriter = StreamContext.Stream.Transport.Output;
      frame.WriteTo(pipeWriter);

      await pipeWriter.FlushAsync(cancellationToken).ConfigureAwait(false);
      TouchActivity();
   }

   /// <summary>
   /// Asynchronously disconnects the client.
   /// </summary>
   public async ValueTask DisconnectAsync()
   {
      if (Interlocked.Exchange(ref _disposedState, 1) == 1) return;

      try
      {
         await StreamContext.Stream.Transport.Output.CompleteAsync();
      }
      catch
      {
         // ignored
      }

      await StreamContext.Stream.DisposeAsync();
   }

   public async ValueTask DisposeAsync()
   {
      await DisconnectAsync();
   }
}
