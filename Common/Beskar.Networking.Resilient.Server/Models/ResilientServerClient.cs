using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Protocol;

namespace Beskar.Networking.Resilient.Server.Models;

/// <summary>
/// Represents a connected client in the resilient server.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientServerClient<TFrame>(NetworkServerStreamContext controlStreamContext)
   : IAsyncDisposable
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// Unique identifier for this connected client.
   /// </summary>
   public Guid Id => Session.Id;

   /// <summary>
   /// The primary control stream context for this client.
   /// </summary>
   public NetworkServerStreamContext ControlStreamContext { get; } = controlStreamContext;

   /// <summary>
   /// The primary control stream for this client.
   /// </summary>
   public INetworkStream ControlStream => ControlStreamContext.Stream;

   /// <summary>
   /// The network session associated with this client.
   /// </summary>
   public INetworkSession Session => ControlStreamContext.Connection.Session;

   /// <summary>
   /// UTC timestamp when the client connected.
   /// </summary>
   public DateTimeOffset ConnectedAt { get; } = DateTimeOffset.UtcNow;

   /// <summary>
   /// UTC timestamp of the last activity (packet received or sent).
   /// </summary>
   public DateTimeOffset LastActivityAt => _lastActivityAt;

   /// <summary>
   /// Indicates whether the underlying transport session is active.
   /// </summary>
   public bool IsConnected => !Session.SessionClosedToken.IsCancellationRequested;

   /// <summary>
   /// Gets all active streams currently open on this client's session.
   /// </summary>
   public IReadOnlyCollection<INetworkStream> ActiveStreams => Session.ActiveStreams;

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
   /// Asynchronously sends a frame on the main control stream.
   /// </summary>
   public ValueTask SendAsync(TFrame frame, CancellationToken cancellationToken = default)
   {
      return SendAsync(frame, ControlStream, cancellationToken);
   }

   /// <summary>
   /// Asynchronously sends a frame on a specific stream in this client's session.
   /// </summary>
   public async ValueTask SendAsync(TFrame frame, INetworkStream stream, CancellationToken cancellationToken = default)
   {
      if (Volatile.Read(ref _disposedState) == 1 || !IsConnected)
         return;

      using var writeLock = await stream.AcquireWriterLock(cancellationToken);

      var pipeWriter = stream.Transport.Output;
      frame.WriteTo(pipeWriter);

      await pipeWriter.FlushAsync(cancellationToken);
      TouchActivity();
   }

   /// <summary>
   /// Asynchronously sends a frame on a stream identified by its StreamId.
   /// </summary>
   public ValueTask SendAsync(TFrame frame, long streamId, CancellationToken cancellationToken = default)
   {
      foreach (var stream in ActiveStreams)
      {
         if (stream.StreamId == streamId)
         {
            return SendAsync(frame, stream, cancellationToken);
         }
      }

      return ValueTask.CompletedTask;
   }

   /// <summary>
   /// Opens a new stream on this client's network session (for multiplexed transports like QUIC).
   /// </summary>
   public ValueTask<Result<INetworkStream, NetworkCodeError>> OpenStreamAsync(
      NetworkStreamDirection direction = NetworkStreamDirection.Bidirectional,
      CancellationToken cancellationToken = default)
   {
      return Session.OpenStreamAsync(direction, cancellationToken);
   }

   /// <summary>
   /// Accepts a new stream on this client's network session (for multiplexed transports like QUIC).
   /// </summary>
   public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(
      CancellationToken cancellationToken = default)
   {
      return Session.AcceptStreamAsync(cancellationToken);
   }

   /// <summary>
   /// Asynchronously disconnects the client session and closes all active streams.
   /// </summary>
   public async ValueTask DisconnectAsync()
   {
      if (Interlocked.Exchange(ref _disposedState, 1) == 1) return;

      try
      {
         await ControlStream.Transport.Output.CompleteAsync();
      }
      catch
      {
         // ignored
      }

      await Session.DisposeAsync();
   }

   public async ValueTask DisposeAsync()
   {
      await DisconnectAsync();
   }
}
