using System.Buffers;
using System.Collections.Concurrent;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Memory.Threading;
using Beskar.Memory.Writers;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Protocol;
using Beskar.Networking.Protocol.Payloads;
using Beskar.Networking.Resilient.Common.Enums;
using Beskar.Networking.Resilient.Common.Interfaces;
using Beskar.Networking.Resilient.Server.Contexts;
using Beskar.Networking.Resilient.Server.Models;
using Beskar.Networking.Resilient.Server.Services;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Resilient.Server;


public sealed class ResilientServer<TFrame>
   : IResilientServer<TFrame>
   where TFrame : struct, IFramingProtocol<TFrame>
{
   public ResilientServerState State
   {
      get => (ResilientServerState)_state;
      private set => _state = (int)value;
   }

   public bool IsRunning
      => State is ResilientServerState.Running;

   public IReadOnlyList<INetworkListener> Listeners
      => _listeners;

   public ResilientServerOptions Options { get; }

   public ResilientServerEvents<TFrame> Events { get; } = new();

   public ResilientServerClients<TFrame> Clients { get; } = new();

   private int _disposedState; // 0 = not disposed, 1 = disposed
   private volatile int _state = (int)ResilientServerState.Stopped;

   private readonly INetworkListener[] _listeners;
   private CancellationTokenSource _cancellationTokenSource = new();

   private readonly ResilientKeepAliveService<TFrame> _keepAliveService;

   public ResilientServer(INetworkListener[] listeners, ResilientServerOptions options)
   {
      _listeners = listeners;
      Options = options;

      _keepAliveService = new ResilientKeepAliveService<TFrame>(this);
   }

   public async Task<VoidResult<StringError>> StartAsync()
   {
      if (Volatile.Read(ref _disposedState) == 1)
         return new StringError("Already disposed server.");

      if (Interlocked.CompareExchange(ref _state, (int)ResilientServerState.Starting, (int)ResilientServerState.Stopped) != (int)ResilientServerState.Stopped)
         return new StringError("Server is already running or starting.");

      try
      {
         await _cancellationTokenSource.CancelAsync();
         _cancellationTokenSource.Dispose();
      }
      catch (ObjectDisposedException)
      {
         // already disposed
      }

      _cancellationTokenSource = new CancellationTokenSource();
      var ct = _cancellationTokenSource.Token;

      TraceLogger.LogServerInfo("ResilientServer: Starting server with {0} listener(s)...", _listeners.Length);
      using var startedBuilder = new ArrayBuilder<INetworkListener>(_listeners.Length);

      foreach (var listener in _listeners)
      {
         var startResult = await listener.BindAsync(ct);
         if (startResult.Failed)
         {
            TraceLogger.LogServerError("ResilientServer: Failed to bind listener on {0}: {1}", listener.LocalAddress, startResult.Error.Message);
            try
            {
               await _cancellationTokenSource.CancelAsync();
            }
            catch
            {
               // ignored
            }

            await CleanupCode(startedBuilder, CancellationToken.None);
            _cancellationTokenSource.Dispose();

            State = ResilientServerState.Stopped;
            return new StringError($"Failed to start one of the listener: {startResult.Error.Message}");
         }

         startedBuilder.Add(listener);
         _ = Task.Run(() => RunAcceptTask(listener, ct), ct);
      }

      await _keepAliveService.StartAsync();
      State = ResilientServerState.Running;
      TraceLogger.LogServerInfo("ResilientServer: Server started successfully. State is Running.");

      if (Events.OnStart.Count > 0)
      {
         await Events.OnStart.ExecuteAsync(
            new ResilientServerStartContext<TFrame> { Server = this },
            HandlerExecutionStrategy.SequentialContinueOnError, ct);
      }

      return true;

      static async Task CleanupCode(ArrayBuilder<INetworkListener> builder, CancellationToken ct)
      {
         var cleanups = builder.WrittenSpan.ToArray();
         foreach (var cleanup in cleanups)
         {
            await cleanup.UnbindAsync(ct);
         }
      }
   }

   public async Task<VoidResult<StringError>> StopAsync()
   {
      if (Volatile.Read(ref _disposedState) == 1)
         return new StringError("Already disposed server.");

      if (Interlocked.CompareExchange(ref _state, (int)ResilientServerState.Stopping, (int)ResilientServerState.Running) != (int)ResilientServerState.Running)
         return new StringError("Server is not running.");

      TraceLogger.LogServerInfo("ResilientServer: Stopping server...");
      State = ResilientServerState.Stopping;

      try
      {
         await _cancellationTokenSource.CancelAsync();
      }
      catch (ObjectDisposedException)
      {
         // already disposed
      }

      var listeners = _listeners.ToArray();
      foreach (var listener in listeners)
      {
         try
         {
            await listener.UnbindAsync();
         }
         catch
         {
            // background exception protection
         }
      }

      await _keepAliveService.StopAsync();
      await Clients.DisconnectAllAsync();

      State = ResilientServerState.Stopped;
      TraceLogger.LogServerInfo("ResilientServer: Server stopped.");

      if (Events.OnStop.Count > 0)
      {
         await Events.OnStop.ExecuteAsync(
            new ResilientServerStopContext<TFrame> { Server = this },
            HandlerExecutionStrategy.SequentialContinueOnError, CancellationToken.None);
      }

      return true;
   }

   private async Task RunAcceptTask(INetworkListener listener, CancellationToken ct)
   {
      while (!ct.IsCancellationRequested)
      {
         try
         {
            var sessionResult = await listener.AcceptSessionAsync(ct);
            if (sessionResult.Failed) continue;

            if (!Options.OpenToNewConnections ||
                (Options.MaxConnections > 0 && Clients.Count >= Options.MaxConnections))
            {
               TraceLogger.LogServerWarning("ResilientServer: Rejected incoming session {0} from {1} (OpenToNewConnections={2}, MaxConnections={3}, Current={4})",
                  sessionResult.Success.Id, sessionResult.Success.RemoteAddress, Options.OpenToNewConnections, Options.MaxConnections, Clients.Count);
               await sessionResult.Success.DisposeAsync();
               continue;
            }

            TraceLogger.LogServerInfo("ResilientServer: Accepted session {0} from {1}", sessionResult.Success.Id, sessionResult.Success.RemoteAddress);
            _ = Task.Factory.StartNew(
               () => RunClientTask(listener, sessionResult.Success, ct),
               CancellationToken.None,
               TaskCreationOptions.PreferFairness,
               TaskScheduler.Default);
         }
         catch (OperationCanceledException)
         {
            break;
         }
         catch (Exception)
         {
            // listener loop protection
         }
      }
   }

   private async Task RunClientTask(INetworkListener listener, INetworkSession session, CancellationToken ct)
   {
      if (ct.IsCancellationRequested || State is not (ResilientServerState.Running or ResilientServerState.Starting))
      {
         await session.DisposeAsync();
         return;
      }

      if (Events.OnPreHandshake.Count > 0)
      {
         var preHandshakeContext = new ResilientPreHandshakeContext<TFrame>
         {
            Listener = listener,
            Session = session,
            CancellationToken = ct
         };

         await Events.OnPreHandshake.ExecuteAsync(
            preHandshakeContext, HandlerExecutionStrategy.SequentialContinueOnError, ct);

         if (preHandshakeContext.IsDenied)
         {
            TraceLogger.LogServerWarning("ResilientServer: Pre-handshake check denied session {0} from {1}", session.Id, session.RemoteAddress);
            await session.DisposeAsync();
            return;
         }
      }

      ResilientServerClient<TFrame>? client = null;
      var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
      var combinedToken = combinedCts.Token;

      try
      {
         var controlStreamResult = await session.AcceptStreamAsync(ct);
         if (controlStreamResult.Failed)
         {
            await session.DisposeAsync();
            return;
         }

         var connectionContext = new NetworkServerConnectionContext(listener, session);
         var controlStreamContext = new NetworkServerStreamContext(connectionContext, controlStreamResult.Success);

         client = new ResilientServerClient<TFrame>(controlStreamContext, Options);
         client.OnDisposing = id => Clients.TryRemove(id, out _);

         if (!Clients.TryAdd(client, Options.MaxConnections))
         {
            await client.DisposeAsync();
            return;
         }

         Task? acceptMultiplexedTask = null;
         if (session.IsSupportingMultiplexing)
         {
            acceptMultiplexedTask = Task.Run(() => RunAcceptMultiplexedStreamsTask(client, connectionContext, combinedToken));
         }

         var listenTask = Task.Run(async () =>
         {
            try
            {
               await RunClientListenTask(client, controlStreamContext, combinedToken);
            }
            finally
            {
               try
               {
                  await combinedCts.CancelAsync();
               }
               catch
               {
                  // expected
               }
            }
         });

         ConnectPacketPayload? connectPayload = null;
         try
         {
            using var handshakeTimeoutCts = new CancellationTokenSource(Options.HandshakeTimeout);
            using var handshakeCts = CancellationTokenSource.CreateLinkedTokenSource(combinedToken, handshakeTimeoutCts.Token);
            connectPayload = await ReadConnectPayloadAsync(client, handshakeCts.Token);
         }
         catch
         {
            // timeout or cancellation
         }

         var handshakeSuccess = false;
         if (connectPayload != null)
         {
            client.ConnectPayload = connectPayload;

            if (Events.OnConnect.Count > 0)
            {
               var connectContext = new ResilientClientConnectContext<TFrame>
               {
                  Client = client,
                  ConnectPayload = connectPayload,
                  CancellationToken = combinedToken
               };

               await Events.OnConnect.ExecuteAsync(
                  connectContext, HandlerExecutionStrategy.SequentialContinueOnError, combinedToken);

               if (!connectContext.IsDenied)
               {
                  handshakeSuccess = true;
               }
            }
            else
            {
               handshakeSuccess = true;
            }
         }

         if (handshakeSuccess)
         {
            var connectAckFrame = TFrame.CreateFrame(ResilientFrameKind.ConnectAcknowledged);
            await client.SendAsync(connectAckFrame, combinedToken);

            client.SetHandshakeResult(true);
            client.DrainingTask = ProcessBufferedPreHandshakeFramesAsync(client, combinedToken);
            TraceLogger.LogServerInfo("ResilientServer: Handshake succeeded for client {0} ({1}). Sent ConnectAck.", client.Id, session.RemoteAddress);
         }
         else
         {
            TraceLogger.LogServerWarning("ResilientServer: Handshake denied or failed for client {0} ({1}). Disconnecting.", client.Id, session.RemoteAddress);
            client.SetHandshakeResult(false);
            await client.DisconnectAsync();

            try
            {
               await combinedCts.CancelAsync();
            }
            catch
            {
               // ignored
            }
         }

         await listenTask;

         if (acceptMultiplexedTask != null)
         {
            await acceptMultiplexedTask;
         }
      }
      catch (Exception)
      {
         // client connection dropped or failed
      }
      finally
      {
         try
         {
            await combinedCts.CancelAsync();
         }
         catch
         {
            // ignored
         }

         if (client != null)
         {
            TraceLogger.LogServerInfo("ResilientServer: Client {0} ({1}) disconnected.", client.Id, session.RemoteAddress);
            client.SetHandshakeResult(false);
            Clients.TryRemove(client.Id, out _);

            if (Events.ClientDisconnected.Count > 0)
            {
               var disconnectContext = new ResilientClientDisconnectedContext<TFrame>
               {
                  Client = client
               };

               try
               {
                  await Events.ClientDisconnected.ExecuteAsync(
                     disconnectContext, HandlerExecutionStrategy.SequentialContinueOnError, CancellationToken.None);
               }
               catch
               {
                  // background exception protection
               }
            }

            await client.DisposeAsync();
         }
         else
         {
            await session.DisposeAsync();
         }

         combinedCts.Dispose();
      }
   }

   private static async ValueTask<ConnectPacketPayload?> ReadConnectPayloadAsync(
      ResilientServerClient<TFrame> client,
      CancellationToken ct)
   {
      try
      {
         var reader = client.ControlPayloadChannel.Reader;
         while (await reader.WaitToReadAsync(ct))
         {
            while (reader.TryRead(out var payload))
            {
               if (payload is ConnectPacketPayload connectPayload)
               {
                  return connectPayload;
               }
            }
         }
      }
      catch
      {
         // ignored
      }

      return null;
   }

   private async Task RunAcceptMultiplexedStreamsTask(
      ResilientServerClient<TFrame> client,
      NetworkServerConnectionContext connectionContext,
      CancellationToken ct)
   {
      var streamTasks = new ConcurrentDictionary<Task, byte>();
      try
      {
         while (!ct.IsCancellationRequested && client.IsConnected)
         {
            try
            {
               var streamResult = await client.Session.AcceptStreamAsync(ct);
               if (streamResult.Failed) break;

               var streamContext = new NetworkServerStreamContext(connectionContext, streamResult.Success);
               var t = Task.Run(() => RunClientListenTask(client, streamContext, ct), ct);

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
               var whenAll = Task.WhenAll(streamTasks.Keys);
               await Task.WhenAny(whenAll, Task.Delay(2000, CancellationToken.None));
            }
            catch
            {
               // protection against stream exceptions
            }
         }
      }
   }

   private async Task RunClientListenTask(
      ResilientServerClient<TFrame> client,
      NetworkServerStreamContext streamContext,
      CancellationToken ct)
   {
      try
      {
         var reader = streamContext.Stream.Transport.Input;

         while (!ct.IsCancellationRequested)
         {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;

            if (result.IsCanceled) break;
            if (buffer.IsEmpty && result.IsCompleted) break;

            var consumed = buffer.Start;
            var examined = buffer.End;

            while (!buffer.IsEmpty)
            {
               TFrame frame;
               SequencePosition consumedPos;

               // Scope SequenceReader so ref struct doesn't cross await boundary
               {
                  var sequenceReader = new SequenceReader<byte>(buffer);
                  if (!TFrame.TryRead(ref sequenceReader, out frame))
                  {
                     // Incomplete frame in buffer, wait for more data from stream
                     break;
                  }
                  consumedPos = sequenceReader.Position;
               }

               client.TouchActivity();
               buffer = buffer.Slice(consumedPos);
               consumed = consumedPos;

               var frameKind = frame.GetFrameKind();

               if (frameKind is ResilientFrameKind.Ping)
               {
                  var pongFrame = TFrame.CreateFrame(ResilientFrameKind.Pong);
                  await client.SendAsync(pongFrame, ct);
               }
               else if (frameKind is ResilientFrameKind.Disconnect)
               {
                  if (frame.TryGetPayload<DisconnectPacketPayload>(out var disconnectPayload) && disconnectPayload != null)
                  {
                     client.DisconnectPayload = disconnectPayload;
                  }
                  else
                  {
                     client.DisconnectPayload = new DisconnectPacketPayload();
                  }
                  await client.DisconnectAsync();
                  break;
               }
               else if (frameKind is ResilientFrameKind.Connect)
               {
                  if (!client.IsHandshakeCompleted)
                  {
                     if (frame.TryGetPayload<ConnectPacketPayload>(out var connectPayload) && connectPayload != null)
                     {
                        client.ControlPayloadChannel.Writer.TryWrite(connectPayload);
                     }
                     else
                     {
                        client.ControlPayloadChannel.Writer.TryWrite(new ConnectPacketPayload());
                     }
                  }
               }
               else if (frameKind is ResilientFrameKind.Authenticate)
               {
                  if (!client.IsHandshakeCompleted)
                  {
                     if (frame.TryGetPayload<AuthenticatePacketPayload>(out var authPayload) && authPayload != null)
                     {
                        client.ControlPayloadChannel.Writer.TryWrite(authPayload);
                     }
                     else
                     {
                        client.ControlPayloadChannel.Writer.TryWrite(new AuthenticatePacketPayload());
                     }
                  }
               }

               var isControlFrame = frameKind is ResilientFrameKind.Connect
                  or ResilientFrameKind.Authenticate
                  or ResilientFrameKind.Ping
                  or ResilientFrameKind.Pong
                  or ResilientFrameKind.Disconnect;

               if (Options.FrameReceivedAllPackets || frameKind is ResilientFrameKind.Message)
               {
                  if (Events.FrameReceived.Count > 0)
                  {
                     if (isControlFrame)
                     {
                        var eventContext = new ResilientFrameReceivedContext<TFrame>
                        {
                           Client = client,
                           Stream = streamContext.Stream,
                           Frame = frame
                        };

                        await Events.FrameReceived.ExecuteAsync(
                           eventContext, HandlerExecutionStrategy.SequentialContinueOnError, ct);
                     }
                     else if (client.IsHandshakeCompleted)
                     {
                        if (!client.DrainingTask.IsCompleted)
                        {
                           await client.DrainingTask;
                        }

                        var eventContext = new ResilientFrameReceivedContext<TFrame>
                        {
                           Client = client,
                           Stream = streamContext.Stream,
                           Frame = frame
                        };

                        await Events.FrameReceived.ExecuteAsync(
                           eventContext, HandlerExecutionStrategy.SequentialContinueOnError, ct);
                     }
                     else if (!client.HandshakeCompletedTask.IsCompleted)
                     {
                        if (!client.PreHandshakeFrameChannel.Writer.TryWrite((frame, streamContext.Stream)))
                        {
                           if (client.IsHandshakeCompleted)
                           {
                              if (!client.DrainingTask.IsCompleted)
                              {
                                 await client.DrainingTask;
                              }

                              var eventContext = new ResilientFrameReceivedContext<TFrame>
                              {
                                 Client = client,
                                 Stream = streamContext.Stream,
                                 Frame = frame
                              };

                              await Events.FrameReceived.ExecuteAsync(
                                 eventContext, HandlerExecutionStrategy.SequentialContinueOnError, ct);
                           }
                        }
                     }
                  }
               }
            }

            reader.AdvanceTo(consumed, examined);
            if (result.IsCompleted && buffer.IsEmpty) break;
         }
      }
      catch (OperationCanceledException)
      {
         // client cancelled or disconnected
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("ResilientServer: Exception in stream listener for client {0}: {1}", client.Id, ex.Message);
      }
      finally
      {
         client.ControlPayloadChannel.Writer.TryComplete();
         await streamContext.Stream.DisposeAsync();
      }
   }

   private async Task ProcessBufferedPreHandshakeFramesAsync(
      ResilientServerClient<TFrame> client,
      CancellationToken ct)
   {
      var reader = client.PreHandshakeFrameChannel.Reader;
      try
      {
         while (await reader.WaitToReadAsync(ct))
         {
            while (reader.TryRead(out var item))
            {
               if (Events.FrameReceived.Count > 0)
               {
                  var eventContext = new ResilientFrameReceivedContext<TFrame>
                  {
                     Client = client,
                     Stream = item.Stream,
                     Frame = item.Frame
                  };

                  await Events.FrameReceived.ExecuteAsync(
                     eventContext, HandlerExecutionStrategy.SequentialContinueOnError, ct);
               }
            }
         }
      }
      catch
      {
         // background protection
      }
   }

   public async ValueTask DisposeAsync()
   {
      if (Interlocked.Exchange(ref _disposedState, 1) == 1) return;

      await StopAsync();

      foreach (var listener in _listeners)
      {
         await listener.DisposeAsync();
      }

      await _keepAliveService.DisposeAsync();
      await Clients.DisposeAsync();
   }
}
