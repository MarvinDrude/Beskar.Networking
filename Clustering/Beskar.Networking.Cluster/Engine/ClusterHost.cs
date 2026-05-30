using System.Buffers;
using System.Collections.Concurrent;
using Beskar.Memory.Code.PacketGenerator.Enums;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Cluster.Protocol.Models;
using Beskar.Networking.Cluster.Protocol.Registries;
using Beskar.Utilities.Tracing;

namespace Beskar.Networking.Cluster.Engine;

public sealed class ClusterHost(
   Guid localNodeId,
   INetworkListener listener,
   ClusterSessionRegistry sessionRegistry,
   ShardRoutingRegistry shardRoutingRegistry,
   ClusterMessageRegistry messageRegistry)
{
   private readonly Guid _localNodeId = localNodeId;
   private readonly INetworkListener _listener = listener;

   private readonly ClusterSessionRegistry _sessionRegistry = sessionRegistry;
   private readonly ShardRoutingRegistry _routingRegistry = shardRoutingRegistry;
   private readonly ClusterMessageRegistry _messageRegistry = messageRegistry;

   private readonly CancellationTokenSource _cts = new();
   private readonly ConcurrentDictionary<Guid, ShardReplica> _localReplicas = [];

   private Task? _listenerLoopTask;
   private int _isRunning;

   public async Task<VoidResult<StringError>> StartAsync(CancellationToken ct = default)
   {
      if (Interlocked.CompareExchange(ref _isRunning, 1, 0) == 1)
      {
         return new StringError("ClusterHost is already running.");
      }

      TraceLogger.LogNeutralInfo("[ClusterHost {0}] Starting listener on {1}", _localNodeId, _listener.LocalAddress);

      var bindResult = await _listener.BindAsync(ct);
      if (bindResult.Failed)
      {
         return new StringError(bindResult.Error.Message);
      }

      _listenerLoopTask = Task.Run(async () => await ListenLoopAsync(_cts.Token), _cts.Token);
      return true;
   }

   public async Task<VoidResult<StringError>> StopAsync(CancellationToken ct = default)
   {
      if (Interlocked.CompareExchange(ref _isRunning, 0, 1) != 1)
      {
         return new StringError("ClusterHost is not running.");
      }

      TraceLogger.LogNeutralInfo("[ClusterHost {0}] Stopping cluster host...", _localNodeId);

      await _cts.CancelAsync();

      if (_listenerLoopTask is not null)
      {
         try
         {
            await _listenerLoopTask;
         }
         catch (Exception)
         {
            // ignored
         }
      }

      var unbindResult = await _listener.UnbindAsync(ct);
      if (unbindResult.Failed)
      {
         return new StringError(unbindResult.Error.Message);
      }

      await _sessionRegistry.DisposeAsync();
      foreach (var replica in _localReplicas.Values)
      {
         await replica.DisposeAsync();
      }

      _localReplicas.Clear();
      return true;
   }

   private async Task ListenLoopAsync(CancellationToken ct)
   {
      while (!ct.IsCancellationRequested)
      {
         try
         {
            var acceptResult = await _listener.AcceptSessionAsync(ct);
            if (acceptResult.Failed)
               continue;

            var session = acceptResult.Success;
            var context = new ClusterMessageContext()
            {
               Session = session,
            };

            _ = Task.Run(() => HandleIncomingSessionAsync(context, ct), ct);
         }
         catch (Exception ex) when (ex is not OperationCanceledException)
         {
            TraceLogger.LogNeutralError("[ClusterHost {0}] Error in listen loop: {1}", _localNodeId, ex.Message);
         }
      }
   }

   private async Task HandleIncomingSessionAsync(ClusterMessageContext context, CancellationToken ct)
   {
      try
      {
         var streamResult = await context.Session.AcceptStreamAsync(ct);
         if (streamResult.Failed)
            return;

         var stream = streamResult.Success;
         var reader = stream.Transport.Input;

         while (!ct.IsCancellationRequested)
         {
            var readResult = await reader.ReadAsync(ct);
            var buffer = readResult.Buffer;

            while (buffer.Length > 0)
            {
               var result = await _messageRegistry.RoutePacket(ref context, buffer, ct);
               if (result.State.IsSuccess && result.ConsumedBytes > 0)
               {
                  buffer = buffer.Slice(result.ConsumedBytes);
               }
               else
               {
                  break;
               }
            }

            reader.AdvanceTo(buffer.Start, readResult.Buffer.End);
            if (readResult.IsCompleted || readResult.IsCanceled)
               break;
         }
      }
      catch (Exception ex) when (ex is not OperationCanceledException)
      {
         TraceLogger.LogNeutralError("[ClusterHost {0}] Error during incoming session handshake: {1}", _localNodeId, ex.Message);
      }
   }
}
