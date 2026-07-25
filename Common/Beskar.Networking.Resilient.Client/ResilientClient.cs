using System.Buffers;
using System.Collections.Concurrent;
using System.Net;
using System.Threading.Channels;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Memory.Threading;
using Beskar.Networking.Abstractions.Enums;
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Protocol.Utilities;
using Beskar.Networking.Resilient.Common.Enums;
using Beskar.Networking.Resilient.Client.Contexts;
using Beskar.Networking.Resilient.Client.Services;

namespace Beskar.Networking.Resilient.Client;

/// <summary>
/// High-performance, event-driven resilient client implementation over any network transport.
/// </summary>
/// <typeparam name="TFrame">The framing protocol struct type.</typeparam>
public sealed class ResilientClient<TFrame> : IAsyncDisposable
   where TFrame : struct, IFramingProtocol<TFrame>
{
   public ResilientClientState State
   {
      get => (ResilientClientState)_state;
      private set => _state = (int)value;
   }

   public bool IsConnected
      => State is ResilientClientState.Connected;

   public INetworkClient NetworkClient { get; }

   public ResilientClientOptions Options { get; }

   public ResilientClientEvents<TFrame> Events { get; } = new();

   public EndPoint? RemoteEndPoint => _remoteEndPoint ?? NetworkClient.RemoteAddress;

   public INetworkSession? Session => NetworkClient.Session;

   public INetworkStream? ControlStream => _controlStream;

   public IReadOnlyCollection<INetworkStream> ActiveStreams => Session?.ActiveStreams ?? Array.Empty<INetworkStream>();

   public DateTimeOffset ConnectedAt { get; private set; }

   public DateTimeOffset LastActivityAt => new(Volatile.Read(ref _lastActivityTicks), TimeSpan.Zero);

   public DisconnectPacketPayload? DisconnectPayload { get; internal set; }

   private int _disposedState;
   private volatile int _state = (int)ResilientClientState.Disconnected;

   private long _lastActivityTicks = DateTimeOffset.UtcNow.Ticks;
   private long _lastTouchMs = Environment.TickCount64;

   private EndPoint? _remoteEndPoint;
   private INetworkStream? _controlStream;

   private CancellationTokenSource? _connectionCts;
   private readonly ResilientClientKeepAliveService<TFrame> _keepAliveService;
   private Channel<IResilientPayload> _handshakeChannel = CreateHandshakeChannel();

   private static Channel<IResilientPayload> CreateHandshakeChannel()
      => Channel.CreateUnbounded<IResilientPayload>(new UnboundedChannelOptions
         { SingleWriter = false, SingleReader = false });

   public ResilientClient(INetworkClient networkClient, ResilientClientOptions? options = null)
   {
      NetworkClient = networkClient;
      Options = options ?? new ResilientClientOptions();
      _keepAliveService = new ResilientClientKeepAliveService<TFrame>(this);
   }

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
   /// Connects the resilient client to the target remote endpoint and performs connection handshake.
   /// </summary>
   public async Task<VoidResult<StringError>> ConnectAsync(EndPoint endPoint,
      CancellationToken cancellationToken = default)
   {
      if (Volatile.Read(ref _disposedState) == 1)
         return new StringError("Already disposed client.");

      if (State is ResilientClientState.Connected or ResilientClientState.Connecting or ResilientClientState.Reconnecting)
         return new StringError("Client is already connected, connecting, or reconnecting.");

      _remoteEndPoint = endPoint;
      State = ResilientClientState.Connecting;

      _handshakeChannel = CreateHandshakeChannel();

      _connectionCts?.Dispose();
      _connectionCts = new CancellationTokenSource();

      using var handshakeTimeoutCts = new CancellationTokenSource(Options.HandshakeTimeout);
      using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(_connectionCts.Token, cancellationToken, handshakeTimeoutCts.Token);
      var ct = linkedCts.Token;

      var result = await ConnectInternalAsync(endPoint, ct);
      if (result.Failed)
      {
         State = ResilientClientState.Disconnected;
         return result;
      }

      return true;
   }

   /// <summary>
   /// Connects the resilient client using the previously configured or known remote endpoint.
   /// </summary>
   public Task<VoidResult<StringError>> ConnectAsync(CancellationToken cancellationToken = default)
   {
      if (_remoteEndPoint == null)
      {
         return Task.FromResult<VoidResult<StringError>>(
            new StringError("No remote EndPoint specified or previously connected."));
      }

      return ConnectAsync(_remoteEndPoint, cancellationToken);
   }

   private async Task<VoidResult<StringError>> ConnectInternalAsync(EndPoint endPoint, CancellationToken ct)
   {
      try
      {
         var connectResult = await NetworkClient.ConnectAsync(endPoint, ct);
         if (connectResult.Failed)
         {
            return new StringError($"Transport connect failed: {connectResult.Error.Message}");
         }

         var session = connectResult.Success;

         var streamResult = await session.OpenStreamAsync(NetworkStreamDirection.Bidirectional, ct);
         if (streamResult.Failed)
         {
            await session.DisposeAsync();
            return new StringError($"Control stream open failed: {streamResult.Error.Message}");
         }

         _controlStream = streamResult.Success;

         // Start background listen task on control stream
         _ = Task.Run(() => RunClientListenTask(_controlStream, _connectionCts!.Token));

         if (session.IsSupportingMultiplexing)
         {
            _ = Task.Run(() => RunAcceptMultiplexedStreamsTask(_connectionCts!.Token));
         }

         // Send Connect frame payload
         var len = Options.ConnectPayload.GetEncodedLength();
         using var writer = new PooledBufferWriter(len);
         if (Options.ConnectPayload.TryWrite(writer.GetSpan(len), out var bytesWritten))
         {
            writer.Advance(bytesWritten);
         }

         var connectFrame =
            TFrame.CreateFrame(ResilientFrameKind.Connect, new ReadOnlySequence<byte>(writer.WrittenMemory));
         await SendAsync(connectFrame, _controlStream, ct);

         // Wait for handshake completion (ConnectAcknowledged or Authenticate challenge)
         var handshakeSuccess = await ProcessHandshakeAsync(ct);
         if (!handshakeSuccess)
         {
            await DisconnectInternalAsync(null, raiseDisconnectedEvent: false);
            State = ResilientClientState.Disconnected;

            return new StringError("Handshake failed, timed out, or denied by server.");
         }

         if (Volatile.Read(ref _disposedState) == 1 || State is ResilientClientState.Disconnecting or ResilientClientState.Disconnected)
         {
            await DisconnectInternalAsync(null, raiseDisconnectedEvent: false);
            State = ResilientClientState.Disconnected;
            return new StringError("Client was disconnected or disposed during connection.");
         }

         State = ResilientClientState.Connected;
         ConnectedAt = DateTimeOffset.UtcNow;
         TouchActivity();

         await _keepAliveService.StartAsync();

         if (Events.OnConnected.Count > 0)
         {
            await Events.OnConnected.ExecuteAsync(
               new ResilientClientConnectedContext<TFrame> { Client = this },
               HandlerExecutionStrategy.SequentialContinueOnError, ct);
         }

         return true;
      }
      catch (Exception ex)
      {
         await DisconnectInternalAsync(null, raiseDisconnectedEvent: false);
         State = ResilientClientState.Disconnected;
         return new StringError($"Connect error: {ex.Message}");
      }
   }

   private async ValueTask<bool> ProcessHandshakeAsync(CancellationToken ct)
   {
      var reader = _handshakeChannel.Reader;

      try
      {
         while (await reader.WaitToReadAsync(ct))
         {
            while (reader.TryRead(out var payload))
            {
               if (payload is ConnectAckPayloadMarker)
               {
                  return true;
               }

               if (payload is AuthenticatePacketPayload challengePayload)
               {
                  if (Events.OnAuthenticate.Count > 0)
                  {
                     var authContext = new ResilientClientAuthenticateContext<TFrame>
                     {
                        Client = this,
                        ChallengePayload = challengePayload,
                        CancellationToken = ct
                     };

                     await Events.OnAuthenticate.ExecuteAsync(
                        authContext, HandlerExecutionStrategy.SequentialContinueOnError, ct);
                  }
               }
            }
         }
      }
      catch (OperationCanceledException)
      {
         // cancelled or timed out
      }

      return false;
   }

   /// <summary>
   /// Asynchronously disconnects the client session and closes all active streams.
   /// Optionally sends a disconnect payload frame to the server first.
   /// </summary>
   public async Task<VoidResult<StringError>> DisconnectAsync(DisconnectPacketPayload? disconnectPayload = null)
   {
      if (Volatile.Read(ref _disposedState) == 1)
         return new StringError("Already disposed client.");

      if (State is ResilientClientState.Disconnected or ResilientClientState.Disconnecting)
         return new StringError("Client is not connected.");

      State = ResilientClientState.Disconnecting;

      await DisconnectInternalAsync(disconnectPayload, raiseDisconnectedEvent: true);

      State = ResilientClientState.Disconnected;
      return true;
   }

   private async ValueTask DisconnectInternalAsync(DisconnectPacketPayload? disconnectPayload, bool raiseDisconnectedEvent = true)
   {
      try
      {
         await _keepAliveService.StopAsync();
      }
      catch
      {
         // ignored
      }

      if (disconnectPayload != null && ControlStream != null &&
          Session is { SessionClosedToken.IsCancellationRequested: false })
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

            var frame = TFrame.CreateFrame(ResilientFrameKind.Disconnect,
               new ReadOnlySequence<byte>(writer.WrittenMemory));
            await SendAsync(frame, ControlStream);
         }
         catch
         {
            // ignored if send fails during disconnect
         }
      }

      if (_connectionCts != null)
      {
         try
         {
            await _connectionCts.CancelAsync();
         }
         catch
         {
            // ignored
         }
      }

      if (raiseDisconnectedEvent && Events.OnDisconnected.Count > 0)
      {
         var disconnectContext = new ResilientClientDisconnectedContext<TFrame>
         {
            Client = this,
            DisconnectPayload = DisconnectPayload
         };

         try
         {
            await Events.OnDisconnected.ExecuteAsync(
               disconnectContext, HandlerExecutionStrategy.SequentialContinueOnError);
         }
         catch
         {
            // background protection
         }
      }

      if (ControlStream != null)
      {
         try
         {
            await ControlStream.Transport.Output.CompleteAsync();
         }
         catch
         {
            // ignored
         }
      }

      if (Session != null)
      {
         try
         {
            await Session.DisposeAsync();
         }
         catch
         {
            // ignored
         }
      }
   }

   /// <summary>
   /// Asynchronously sends a frame on the main control stream.
   /// </summary>
   public ValueTask SendAsync(TFrame frame, CancellationToken cancellationToken = default)
   {
      if (ControlStream == null)
      {
         throw new InvalidOperationException("ControlStream is not initialized. Connect client first.");
      }

      return SendAsync(frame, ControlStream, cancellationToken);
   }

   /// <summary>
   /// Asynchronously sends a frame on a specific stream in this client's session.
   /// </summary>
   public async ValueTask SendAsync(TFrame frame, INetworkStream stream, CancellationToken cancellationToken = default)
   {
      if (Volatile.Read(ref _disposedState) == 1 || Session == null ||
          Session.SessionClosedToken.IsCancellationRequested)
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
      if (ControlStream == null)
      {
         throw new InvalidOperationException("ControlStream is not initialized. Connect client first.");
      }

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
      var s = serializer ?? Options.Serializer;
      if (s == null)
      {
         throw new InvalidOperationException(
            "No IResilientSerializer provided or configured on ResilientClientOptions.");
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
      var s = serializer ?? Options.Serializer;
      if (s == null)
      {
         throw new InvalidOperationException(
            "No IResilientSerializer provided or configured on ResilientClientOptions.");
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
      if (Session == null)
      {
         return ValueTask.FromResult<Result<INetworkStream, NetworkCodeError>>(
            new NetworkCodeError(-1, "Not connected."));
      }

      return Session.OpenStreamAsync(direction, cancellationToken);
   }

   /// <summary>
   /// Accepts a new stream on this client's network session (for multiplexed transports like QUIC).
   /// </summary>
   public ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptStreamAsync(
      CancellationToken cancellationToken = default)
   {
      if (Session == null)
      {
         return ValueTask.FromResult<Result<INetworkStream, NetworkCodeError>>(
            new NetworkCodeError(-1, "Not connected."));
      }

      return Session.AcceptStreamAsync(cancellationToken);
   }

   private async Task RunAcceptMultiplexedStreamsTask(CancellationToken ct)
   {
      var streamTasks = new ConcurrentDictionary<Task, byte>();
      try
      {
         while (!ct.IsCancellationRequested && Session != null && !Session.SessionClosedToken.IsCancellationRequested)
         {
            try
            {
               var streamResult = await Session.AcceptStreamAsync(ct);
               if (streamResult.Failed) break;

               var t = Task.Run(() => RunClientListenTask(streamResult.Success, ct), ct);

               streamTasks.TryAdd(t, 0);
               _ = t.ContinueWith(_ => streamTasks.TryRemove(t, out var _), TaskScheduler.Default);
            }
            catch
            {
               break;
            }
         }
      }
      finally
      {
         if (!streamTasks.IsEmpty)
         {
            try
            {
               await Task.WhenAll(streamTasks.Keys);
            }
            catch
            {
               // protection against stream exceptions
            }
         }
      }
   }

   private async Task RunClientListenTask(INetworkStream stream, CancellationToken ct)
   {
      try
      {
         var reader = stream.Transport.Input;

         while (!ct.IsCancellationRequested)
         {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;

            if (result.IsCanceled) break;

            var consumed = buffer.Start;
            var examined = buffer.End;

            while (!buffer.IsEmpty)
            {
               TFrame frame;
               SequencePosition consumedPos;

               {
                  var sequenceReader = new SequenceReader<byte>(buffer);
                  if (!TFrame.TryRead(ref sequenceReader, out frame))
                  {
                     break;
                  }

                  consumedPos = sequenceReader.Position;
               }

               TouchActivity();
               buffer = buffer.Slice(consumedPos);
               consumed = consumedPos;

               var frameKind = frame.GetFrameKind();

               if (frameKind is ResilientFrameKind.Ping)
               {
                  var pongFrame = TFrame.CreateFrame(ResilientFrameKind.Pong);
                  await SendAsync(pongFrame, stream, ct);
               }
               else if (frameKind is ResilientFrameKind.Pong)
               {
                  TouchActivity();
               }
               else if (frameKind is ResilientFrameKind.ConnectAcknowledged)
               {
                  if (State is ResilientClientState.Connecting)
                  {
                     _handshakeChannel.Writer.TryWrite(new ConnectAckPayloadMarker());
                  }
               }
               else if (frameKind is ResilientFrameKind.Authenticate)
               {
                  if (State is ResilientClientState.Connecting)
                  {
                     if (frame.TryGetPayload<AuthenticatePacketPayload>(out var authPayload) && authPayload != null)
                     {
                        _handshakeChannel.Writer.TryWrite(authPayload);
                     }
                  }
               }
               else if (frameKind is ResilientFrameKind.Disconnect)
               {
                  if (frame.TryGetPayload<DisconnectPacketPayload>(out var disconnectPayload) &&
                      disconnectPayload != null)
                  {
                     DisconnectPayload = disconnectPayload;
                  }

                  State = ResilientClientState.Disconnecting;
                  _ = DisconnectInternalAsync(null);
                  break;
               }

               if (Options.FrameReceivedAllPackets || frameKind is ResilientFrameKind.Message)
               {
                  if (Events.FrameReceived.Count > 0)
                  {
                     var eventContext = new ResilientClientFrameReceivedContext<TFrame>
                     {
                        Client = this,
                        Stream = stream,
                        Frame = frame
                     };

                     await Events.FrameReceived.ExecuteAsync(
                        eventContext, HandlerExecutionStrategy.SequentialContinueOnError, ct);
                  }
               }
            }

            reader.AdvanceTo(consumed, examined);
            if (result.IsCompleted && buffer.IsEmpty)
            {
               if (stream == ControlStream && State is ResilientClientState.Connected or ResilientClientState.Reconnecting)
               {
                  _ = TriggerAutoReconnectAsync(null);
               }

               break;
            }
         }
      }
      catch (OperationCanceledException)
      {
         // cancelled
      }
      catch (Exception ex)
      {
         if (stream == ControlStream && State is ResilientClientState.Connected or ResilientClientState.Reconnecting)
         {
            _ = TriggerAutoReconnectAsync(ex);
         }
      }
      finally
      {
         if (stream == ControlStream)
         {
            _handshakeChannel.Writer.TryComplete();
         }
         await stream.DisposeAsync();
      }
   }

   private int _isReconnectingState;

   private async Task TriggerAutoReconnectAsync(Exception? cause)
   {
      if (State is ResilientClientState.Disconnecting or ResilientClientState.Disconnected)
         return;

      if (Interlocked.CompareExchange(ref _isReconnectingState, 1, 0) == 1)
         return;

      try
      {
         if (!Options.Reconnecting.AutoReconnect || _remoteEndPoint == null)
         {
            State = ResilientClientState.Disconnected;
            await DisconnectInternalAsync(null, raiseDisconnectedEvent: true);
            return;
         }

         State = ResilientClientState.Reconnecting;
         await DisconnectInternalAsync(null, raiseDisconnectedEvent: false);

         using var masterCts = new CancellationTokenSource();
         var masterCt = masterCts.Token;

         var attempt = 0;
         var maxRetries = Options.Reconnecting.MaxRetries;

         while (!masterCt.IsCancellationRequested && State is ResilientClientState.Reconnecting)
         {
            attempt++;
            if (maxRetries > 0 && attempt > maxRetries)
            {
               break;
            }

            if (Events.OnReconnecting.Count > 0)
            {
               var reconnectContext = new ResilientClientReconnectingContext<TFrame>
               {
                  Client = this,
                  Attempt = attempt,
                  LastException = cause
               };

               await Events.OnReconnecting.ExecuteAsync(
                  reconnectContext, HandlerExecutionStrategy.SequentialContinueOnError, masterCt);
            }

            try
            {
               await Task.Delay(Options.Reconnecting.RetryInterval, masterCt);
            }
            catch (OperationCanceledException)
            {
               break;
            }

            if (State is not ResilientClientState.Reconnecting || Volatile.Read(ref _disposedState) == 1)
            {
               break;
            }

            _connectionCts?.Dispose();
            _connectionCts = CancellationTokenSource.CreateLinkedTokenSource(masterCt);
            var attemptCt = _connectionCts.Token;

            var result = await ConnectInternalAsync(_remoteEndPoint, attemptCt);
            if (!result.Failed)
            {
               if (Volatile.Read(ref _disposedState) == 1 || State is ResilientClientState.Disconnecting or ResilientClientState.Disconnected)
               {
                  await DisconnectInternalAsync(null, raiseDisconnectedEvent: false);
                  State = ResilientClientState.Disconnected;
                  return;
               }

               return; // Reconnect successful!
            }
         }

         State = ResilientClientState.Disconnected;
         await DisconnectInternalAsync(null, raiseDisconnectedEvent: true);
      }
      finally
      {
         Interlocked.Exchange(ref _isReconnectingState, 0);
      }
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposedState, 1) == 1) return;

      State = ResilientClientState.Disconnected;

      if (_connectionCts is not null)
      {
         try
         {
            await _connectionCts.CancelAsync();
         }
         catch
         {
            // ignored if already canceled or disposed
         }
      }

      await DisconnectInternalAsync(null);

      _connectionCts?.Dispose();
      _connectionCts = null;

      await NetworkClient.DisposeAsync();
   }

   private sealed class ConnectAckPayloadMarker : IResilientPayload
   {
   }
}
