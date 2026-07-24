using System.Buffers;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Memory.Threading;
using Beskar.Memory.Writers;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Networking.Protocol;
using Beskar.Networking.Resilient.Common.Enums;
using Beskar.Networking.Resilient.Common.Interfaces;
using Beskar.Networking.Resilient.Common.Packets;
using Beskar.Networking.Resilient.Server.Contexts;
using Beskar.Networking.Resilient.Server.Models;
using Beskar.Networking.Resilient.Server.Services;

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

      if (State is not ResilientServerState.Stopped)
         return new StringError("Server is not running.");

      State = ResilientServerState.Starting;

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

      using var startedBuilder = new ArrayBuilder<INetworkListener>(_listeners.Length);

      foreach (var listener in _listeners)
      {
         var startResult = await listener.BindAsync(ct);
         _ = Task.Run(() => RunAcceptTask(listener, ct), ct);

         if (!startResult.Failed)
         {
            startedBuilder.Add(listener);
            continue;
         }

         await CleanupCode(startedBuilder, ct);
         return new StringError($"Failed to start one of the listener: {startResult.Error.Message}");
      }

      await _keepAliveService.StartAsync();
      State = ResilientServerState.Running;

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

      if (State is not ResilientServerState.Running)
         return new StringError("Server is not running.");

      State = ResilientServerState.Stopping;

      try
      {
         await _keepAliveService.StopAsync();
      }
      catch (Exception)
      {
         // ignored
      }

      try
      {
         await _cancellationTokenSource.CancelAsync();
         _cancellationTokenSource.Dispose();
      }
      catch (ObjectDisposedException)
      {
         // already disposed
      }

      foreach (var listener in _listeners)
      {
         await listener.UnbindAsync();
      }

      await Clients.DisconnectAllAsync();

      State = ResilientServerState.Stopped;

      if (Events.OnStop.Count > 0)
      {
         await Events.OnStop.ExecuteAsync(
            new ResilientServerStopContext<TFrame> { Server = this },
            HandlerExecutionStrategy.SequentialContinueOnError);
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
               await sessionResult.Success.DisposeAsync();
               continue;
            }

            _ = Task.Factory.StartNew(
               () => RunClientTask(listener, sessionResult.Success, ct),
               TaskCreationOptions.PreferFairness);
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
      if (ct.IsCancellationRequested || State is not ResilientServerState.Running)
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
            await session.DisposeAsync();
            return;
         }
      }

      ResilientServerClient<TFrame>? client = null;
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

         client = new ResilientServerClient<TFrame>(controlStreamContext);
         if (!Clients.TryAdd(client))
         {
            await client.DisposeAsync();
            return;
         }

         using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(ct);
         var combinedToken = combinedCts.Token;

         if (session.IsSupportingMultiplexing)
         {
            _ = Task.Run(() => RunAcceptMultiplexedStreamsTask(client, connectionContext, combinedToken), combinedToken);
         }

         var listenTask = Task.Run(() => RunClientListenTask(client, controlStreamContext, combinedToken), combinedToken);

         var connectPayload = await ReadConnectPayloadAsync(client, combinedToken);
         if (connectPayload != null)
         {
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

               if (connectContext.IsDenied)
               {
                  await client.DisconnectAsync();
                  return;
               }
            }

            var connectAckFrame = TFrame.CreateFrame(ResilientFrameKind.ConnectAcknowledged);
            await client.SendAsync(connectAckFrame, combinedToken);
         }

         await listenTask;
      }
      catch (Exception)
      {
         // client connection dropped or failed
      }
      finally
      {
         if (client != null)
         {
            Clients.TryRemove(client.Id, out _);
            await client.DisposeAsync();
         }
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
      while (!ct.IsCancellationRequested && client.IsConnected)
      {
         try
         {
            var streamResult = await client.Session.AcceptStreamAsync(ct);
            if (streamResult.Failed) break;

            var streamContext = new NetworkServerStreamContext(connectionContext, streamResult.Success);
            _ = Task.Run(() => RunClientListenTask(client, streamContext, ct), ct);
         }
         catch
         {
            break;
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
                  client.DisconnectPayload = new DisconnectPacketPayload();
                  await client.DisconnectAsync();
                  break;
               }
               else if (frameKind is ResilientFrameKind.Connect)
               {
                  var connectPayload = new ConnectPacketPayload();
                  client.ControlPayloadChannel.Writer.TryWrite(connectPayload);
               }
               else if (frameKind is ResilientFrameKind.Authenticate)
               {
                  var authPayload = new AuthenticatePacketPayload();
                  client.ControlPayloadChannel.Writer.TryWrite(authPayload);
               }

               if (Options.FrameReceivedAllPackets || frameKind == ResilientFrameKind.Message)
               {
                  if (Events.FrameReceived.Count > 0)
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
      catch (Exception)
      {
         // transport read exception
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
