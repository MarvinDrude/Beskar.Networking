using System.Buffers;
using System.Text;
using Beskar.Memory.Results;
using Beskar.Memory.Results.Errors;
using Beskar.Memory.Threading;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Common.Handlers;
using Beskar.Mqtt.Common.Parsers;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Protocol.Packets;
using Beskar.Mqtt.Protocol.Parsing.Results;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Mqtt.Server.Enums;
using Beskar.Mqtt.Server.Extensions;
using Beskar.Mqtt.Server.Handlers;
using Beskar.Mqtt.Server.Internal;
using Beskar.Mqtt.Server.Options;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Utilities.Tracing;

namespace Beskar.Mqtt.Server;

/// <summary>
/// Runs a complete MQTT server.
/// </summary>
public sealed partial class MqttServer : IAsyncDisposable
{
   /// <summary>
   /// The current running state of the server.
   /// </summary>
   public MqttServerState State
   {
      get => (MqttServerState)_state;
      private set => _state = (int)value;
   }

   /// <summary>
   /// Whether the server is open to new connections.
   /// False = no new clients can connect.
   /// </summary>
   public bool OpenToNewConnections { get; set; } = true;

   /// <summary>
   /// Container for all server events that can be subscribed to.
   /// </summary>
   public ServerEvents Events { get; } = new();

   private volatile bool _disposed;
   private volatile int _state = (int)MqttServerState.Stopped;

   private readonly MqttServerOptions _options;
   private readonly INetworkListener[] _listeners;
   private CancellationTokenSource _cancellationTokenSource = new();

   private readonly MqttClientSessions _clientSessions = new();

   internal MqttServer(INetworkListener[] listeners, MqttServerOptions options)
   {
      _listeners = listeners;
      _options = options;
   }

   public async Task<VoidResult<StringError>> StartAsync()
   {
      if (_disposed)
         return new StringError("Already disposed server.");

      if (State is not MqttServerState.Stopped)
         return new StringError("Server is not in stopped state.");

      State = MqttServerState.Starting;

      await _cancellationTokenSource.CancelAsync();
      _cancellationTokenSource.Dispose();

      _cancellationTokenSource = new CancellationTokenSource();
      var ct = _cancellationTokenSource.Token;

      using var startedBuilder = new ArrayBuilder<INetworkListener>(_listeners.Length);

      foreach (var listener in _listeners)
      {
         var startResult = await listener.BindAsync(ct);
         _ = Task.Run(() => RunAcceptTask(listener, ct), ct);

         if (!startResult.Failed) continue;

         await CleanupCode(startedBuilder, ct);
         return new StringError($"Failed to start one of the listener: {startResult.Error.Message}");
      }

      State = MqttServerState.Running;
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

   public async Task<VoidResult<StringError>> StopAsync(DisconnectOptions? options = null)
   {
      if (_disposed)
         return new StringError("Already disposed server.");

      if (State is not MqttServerState.Running)
         return new StringError("Server is not running.");

      State = MqttServerState.Stopping;
      options ??= new DisconnectOptions()
      {
         ReasonCode = DisconnectReasonCode.ServerShuttingDown
      };

      await _cancellationTokenSource.CancelAsync();
      _cancellationTokenSource.Dispose();

      // notify clients

      foreach (var listener in _listeners)
      {
         await listener.UnbindAsync();
      }

      State = MqttServerState.Stopped;
      return true;
   }

   private async Task RunAcceptTask(INetworkListener listener, CancellationToken ct)
   {
      while (!ct.IsCancellationRequested)
      {
         try
         {
            var session = await listener.AcceptSessionAsync(ct);
            if (session.Failed) continue;

            _ = Task.Factory.StartNew(
               () => RunClientTask(listener, session.Success, ct), TaskCreationOptions.PreferFairness);
         }
         catch (Exception)
         {
            // ignored
         }
      }
   }

   private async Task RunClientTask(INetworkListener listener, INetworkSession session, CancellationToken ct)
   {
      if (ct.IsCancellationRequested) return;
      if (State is MqttServerState.Stopping or MqttServerState.Stopped) return;
      if (OpenToNewConnections) return;

      var controlStream = await session.AcceptStreamAsync(ct);
      if (controlStream.Failed)
      {
         await session.DisposeAsync();
         return;
      }

      MqttServerClient? client = null;
      ServerPacketHandler? packetHandler = null;

      try
      {
         var connectionContext = new NetworkServerConnectionContext(listener, session);
         var streamContext = new NetworkServerStreamContext(connectionContext, controlStream.Success);

         client = _serverClientPool.Get(null);
         client.Initialize(streamContext, _options);

         packetHandler = _packetHandlerPool.Get(null);
         packetHandler.Initialize(this, client);

         _ = Task.Factory.StartNew(
            () => RunClientConnectionTask(client, streamContext, packetHandler, ct),
            TaskCreationOptions.PreferFairness);

         await RunClientListenTask(client, streamContext, packetHandler, (ct) => Task.CompletedTask, ct);
      }
      catch (Exception)
      {
         await session.DisposeAsync();
      }
      finally
      {
         if (client is not null) _serverClientPool.Return(client);
         if (packetHandler is not null) _packetHandlerPool.Return(packetHandler);
      }
   }

   private async Task RunClientConnectionTask(
      MqttServerClient client, NetworkServerStreamContext streamContext, IPacketHandler packetHandler,
      CancellationToken ct)
   {
      try
      {
         var options = await client.ReceiveControlPacketAsync("CONNECT", ct);
         if (options is not ConnectOptions connectOptions)
         {
            await streamContext.Connection.Session.DisposeAsync();
            return;
         }

         var context = new MqttConnectInterceptContext()
         {
            CancellationToken = ct,
            ConnectOptions = connectOptions,
            NetworkSession = streamContext.Connection.Session
         };

         await Events.OnConnectIntercept.ExecuteAsync(
            context, HandlerExecutionStrategy.SequentialContinueOnError, ct);

         if (connectOptions.ClientIdUtf8Bytes.IsEmpty && client.ProtocolVersion is MqttProtocolVersion.V50)
         {
            connectOptions.ClientIdUtf8Bytes = context.AssignedClientIdentifierUtf8Bytes;
         }

         if (connectOptions.ClientIdUtf8Bytes.IsEmpty)
         {
            context.ReasonCode = ConnectReasonCode.ClientIdentifierNotValid;
         }

         var connAck = new ConnAckPacket()
         {
            ResponseInfoUtf8Bytes = ReadOnlySequence<byte>.Empty,
            ReceiveMaximum = 0,
            MaximumPacketSize = 0,
            MaximumQualityOfService = QualityOfServiceType.ExactlyOnce,

            ReturnCode = context.ReasonCode.ToReturnCode,
            ReasonCode = context.ReasonCode,
            TopicAliasMaximum = ushort.MaxValue,
            IsRetainAvailable = true,
            IsSubscriptionIdentifierAvailable = true,
            IsSharedSubscriptionAvailable = false,
            IsWildcardSubscriptionAvailable = true,

            PropertiesBytes = new ReadOnlySequence<byte>(context.ResponseUserProperties.WrittenMemory),
            ServerReferenceUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(context.ServerReference)),
            ReasonStringUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(context.ReasonString)),
            AuthenticationMethodUtf8Bytes = new ReadOnlySequence<byte>(connectOptions.AuthenticationMethodUtf8Bytes),
            AuthenticationDataBytes = new ReadOnlySequence<byte>(context.ResponseAuthenticationData)
         };

         if (context.ReasonCode is not ConnectReasonCode.Success)
         {
            await streamContext.Stream.Send(in connAck, client.ProtocolVersion, ct);
            return;
         }


      }
      catch (Exception)
      {
         // ignored
      }
   }



   private async Task RunClientListenTask(
      MqttServerClient client, NetworkServerStreamContext streamContext, IPacketHandler packetHandler,
      Func<CancellationToken, Task> disconnectHandler, CancellationToken ct)
   {
      try
      {
         // duplex input for reading incoming messages
         var reader = streamContext.Stream.Transport.Input;

         while (true)
         {
            var result = await reader.ReadAsync(ct);
            var buffer = result.Buffer;

            if (result.IsCanceled) break;
            if (buffer.IsEmpty && result.IsCompleted) break;

            var consumed = buffer.Start;
            var examined = buffer.End;

            while (!buffer.IsEmpty)
            {
               var sequenceReader = new SequenceReader<byte>(buffer);
               var parser = new PacketParser(streamContext.Stream, packetHandler, MqttProtocolVersion.Unknown);
               var valueTask = parser.TryDispatch(ref sequenceReader, out var parsedBytes, ct);

               var parseResult = valueTask.IsCompletedSuccessfully
                  ? valueTask.Result
                  : await valueTask.ConfigureAwait(false);

               if (parseResult.Failed || parseResult.Success is PacketDispatchResult.ProtocolError
                      or PacketDispatchResult.InvalidPacketType)
               {
                  // Protocol violation: exit the loop to drop the connection
                  TraceLogger.LogClientError("MqttServer: Protocol violation or parser error (Result: {0}). Exiting receive loop.",
                     parseResult.Failed ? parseResult.Error.Detail : parseResult.Success);
                  return;
               }

               if (parseResult.Success is PacketDispatchResult.NotEnoughData)
               {
                  break;
               }

               consumed = buffer.GetPosition(parsedBytes);
               buffer = buffer.Slice(consumed);
            }

            reader.AdvanceTo(consumed, examined);
            if (result.IsCompleted && buffer.IsEmpty) break;
         }
      }
      catch (OperationCanceledException)
      {
         TraceLogger.LogServerInfo("MqttServer: Message receiver loop cancelled.");
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("MqttServer: Connection drop or reset in receiver loop: {0}", ex.Message);
      }
      finally
      {
         TraceLogger.LogServerInfo("MqttServer: Message receiver loop finished.");
         await disconnectHandler(ct);
      }
   }

   public async ValueTask DisposeAsync()
   {
      if (_disposed) return;
      _disposed = true;

      await StopAsync();

      foreach (var listener in _listeners)
      {
         await listener.DisposeAsync();
      }
   }
}
