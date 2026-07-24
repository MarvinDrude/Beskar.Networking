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

         await RunClientListenTask(client, controlStreamContext, combinedToken);
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
