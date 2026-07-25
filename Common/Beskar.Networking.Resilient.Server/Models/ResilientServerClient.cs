using System.Buffers;
using System.Threading.Channels;
using Beskar.Memory.Results;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Protocol.Utilities;

namespace Beskar.Networking.Resilient.Server.Models;

/// <summary>
/// Represents a connected client in the resilient server.
/// </summary>
/// <typeparam name="TFrame">The protocol framing struct type.</typeparam>
public sealed class ResilientServerClient<TFrame>(
   NetworkServerStreamContext controlStreamContext,
   ResilientServerOptions? options = null)
   : IAsyncDisposable
   where TFrame : struct, IFramingProtocol<TFrame>
{
   /// <summary>
   /// Unique identifier for this connected client.
   /// </summary>
   public Guid Id => Session.Id;

   /// <summary>
   /// The server options associated with this client's server, if available.
   /// </summary>
   public ResilientServerOptions? Options { get; } = options;

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
   public DateTimeOffset LastActivityAt => new(Volatile.Read(ref _lastActivityTicks), TimeSpan.Zero);

   /// <summary>
   /// Indicates whether the underlying transport session is active.
   /// </summary>
   public bool IsConnected => !Session.SessionClosedToken.IsCancellationRequested;

   /// <summary>
   /// Indicates whether the handshake and OnConnect pipeline have completed successfully.
   /// </summary>
   public bool IsHandshakeCompleted
   {
      get => _isHandshakeCompleted;
      internal set => _isHandshakeCompleted = value;
   }

   /// <summary>
   /// Gets all active streams currently open on this client's session.
   /// </summary>
   public IReadOnlyCollection<INetworkStream> ActiveStreams => Session.ActiveStreams;

   /// <summary>
   /// Gets the payload received for Connect, if any.
   /// </summary>
   public ConnectPacketPayload? ConnectPayload { get; internal set; }

   /// <summary>
   /// Gets the payload received or sent for Disconnect, if any.
   /// </summary>
   public DisconnectPacketPayload? DisconnectPayload { get; internal set; }

   /// <summary>
   /// Bounded channel holding control payloads (Connect, Authenticate, etc.).
   /// </summary>
   public Channel<IResilientPayload> ControlPayloadChannel { get; } = Channel.CreateBounded<IResilientPayload>(
      new BoundedChannelOptions(1024)
      {
         FullMode = BoundedChannelFullMode.DropOldest,
         SingleWriter = false,
         SingleReader = false
      });

   internal Action<Guid>? OnDisposing { get; set; }

   /// <summary>
   /// Channel holding pre-handshake message frames for sequential FIFO processing upon handshake completion.
   /// </summary>
   internal Channel<(TFrame Frame, INetworkStream Stream)> PreHandshakeFrameChannel { get; } = Channel.CreateBounded<(TFrame Frame, INetworkStream Stream)>(
      new BoundedChannelOptions(1024)
      {
         SingleWriter = false,
         SingleReader = true,
         FullMode = BoundedChannelFullMode.DropOldest
      });

   /// <summary>
   /// A task that completes when all buffered pre-handshake frames have been drained and processed.
   /// </summary>
   public Task DrainingTask { get; internal set; } = Task.CompletedTask;

   /// <summary>
   /// A task that completes when the client's connection handshake and OnConnect event pipeline have finished.
   /// Evaluates to true if accepted, false if denied or disconnected.
   /// </summary>
   public Task<bool> HandshakeCompletedTask => _handshakeTcs.Task;
   private readonly TaskCompletionSource<bool> _handshakeTcs = new(TaskCreationOptions.RunContinuationsAsynchronously);

   internal void SetHandshakeResult(bool success)
   {
      _isHandshakeCompleted = success;

      _handshakeTcs.TrySetResult(success);
      ControlPayloadChannel.Writer.TryComplete();
      PreHandshakeFrameChannel.Writer.TryComplete();
   }

   private long _lastActivityTicks = DateTimeOffset.UtcNow.Ticks;
   private long _lastTouchMs = Environment.TickCount64;

   private volatile bool _isHandshakeCompleted;
   private int _disposedState;

   /// <summary>
   /// Updates the last activity timestamp. Throttled to prevent high system call overhead per frame.
   /// </summary>
   public void TouchActivity()
   {
      var currentMs = Environment.TickCount64;
      if (currentMs - Volatile.Read(ref _lastTouchMs) >= 200)
      {
         Volatile.Write(ref _lastTouchMs, currentMs);
         Interlocked.Exchange(ref _lastActivityTicks, DateTimeOffset.UtcNow.Ticks);
      }
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
      if (!IsConnected)
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
   /// Asynchronously serializes and sends a generic payload on the main control stream using the configured or provided serializer.
   /// Uses PooledBufferWriter backed by ArrayPool<byte>.Shared for zero-allocation performance.
   /// </summary>
   public ValueTask SendPayloadAsync<TPayload>(
      TPayload payload,
      ResilientFrameKind kind = ResilientFrameKind.Message,
      IResilientSerializer? serializer = null,
      CancellationToken cancellationToken = default)
   {
      return SendPayloadAsync(payload, ControlStream, kind, serializer, cancellationToken);
   }

   /// <summary>
   /// Asynchronously serializes and sends a generic payload on a specific stream using the configured or provided serializer.
   /// Uses PooledBufferWriter backed by ArrayPool<byte>.Shared for zero-allocation performance.
   /// </summary>
   public async ValueTask SendPayloadAsync<TPayload>(
      TPayload payload,
      INetworkStream stream,
      ResilientFrameKind kind = ResilientFrameKind.Message,
      IResilientSerializer? serializer = null,
      CancellationToken cancellationToken = default)
   {
      var s = serializer ?? Options?.Serializer;
      if (s == null)
      {
         throw new InvalidOperationException("No IResilientSerializer provided or configured on ResilientServerOptions.");
      }

      using var writer = new PooledBufferWriter();
      s.Serialize(payload, writer);

      var frame = TFrame.CreateFrame(kind, new ReadOnlySequence<byte>(writer.WrittenMemory));
      await SendAsync(frame, stream, cancellationToken);
   }

   /// <summary>
   /// Deserializes a payload of type <typeparamref name="TPayload"/> from a frame using the configured or provided serializer.
   /// </summary>
   public TPayload? DeserializePayload<TPayload>(
      TFrame frame,
      IResilientSerializer? serializer = null)
   {
      var s = serializer ?? Options?.Serializer;
      if (s == null)
      {
         throw new InvalidOperationException("No IResilientSerializer provided or configured on ResilientServerOptions.");
      }

      var payloadSeq = frame.GetPayloadSequence();
      return s.Deserialize<TPayload>(in payloadSeq);
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
   /// Optionally sends a disconnect payload frame to the client first.
   /// </summary>
   public async ValueTask DisconnectAsync(DisconnectPacketPayload? disconnectPayload = null)
   {
      _handshakeTcs.TrySetResult(false);

      if (Interlocked.Exchange(ref _disposedState, 1) == 1)
      {
         return;
      }

      OnDisposing?.Invoke(Id);

      if (disconnectPayload != null)
      {
         DisconnectPayload = disconnectPayload;

         try
         {
            var len = disconnectPayload.GetEncodedLength();
            using var writer = new PooledBufferWriter(len);
            if (disconnectPayload.TryWrite(writer.GetSpan(len), out var bytesWritten))
            {
               writer.Advance(bytesWritten);
            }

            var frame = TFrame.CreateFrame(ResilientFrameKind.Disconnect, new ReadOnlySequence<byte>(writer.WrittenMemory));
            using var disconnectSendCts = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            await SendAsync(frame, ControlStream, disconnectSendCts.Token);

            await ControlStream.Transport.Output.CompleteAsync();
            await Task.Yield();
         }
         catch
         {
            // ignored if send fails during disconnect
         }
      }

      await Session.DisposeAsync();
   }

   public async ValueTask DisposeAsync()
   {
      await DisconnectAsync();
   }
}
