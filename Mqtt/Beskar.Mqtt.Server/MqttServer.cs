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
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Utilities.Tracing;
using Beskar.Mqtt.Protocol.Extensions;

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

   internal MqttTrieSubscriptionRouter SubscriptionRouter { get; } = new();

   private volatile bool _disposed;
   private volatile int _state = (int)MqttServerState.Stopped;

   private readonly INetworkListener[] _listeners;
   private CancellationTokenSource _cancellationTokenSource = new();

   internal MqttClientSessions ClientSessions { get; }
   internal MqttServerOptions Options { get; }

   internal MqttServer(INetworkListener[] listeners, MqttServerOptions options)
   {
      _listeners = listeners;
      Options = options;

      ClientSessions = new MqttClientSessions(this);
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
      if (!OpenToNewConnections) return;

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
         client.Initialize(streamContext, Options);

         using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, client.CancellationToken);
         var combinedToken = combinedCts.Token;

         packetHandler = _packetHandlerPool.Get(null);
         packetHandler.Initialize(this, client);

         _ = Task.Factory.StartNew(
            () => RunClientConnectionTask(client, streamContext, packetHandler, combinedToken),
            TaskCreationOptions.PreferFairness);

         await RunClientListenTask(client, streamContext, packetHandler,
            async (_) => await session.DisposeAsync(), combinedToken);
      }
      catch (Exception)
      {
         await session.DisposeAsync();
      }
      finally
      {
         try
         {
            if (Events.OnDisconnect.Count > 0 && client is not null)
            {
               var disconnectOptions = client.DisconnectOptions;
               var grace = ClientDisconnectKind.Ungraceful;

               if (disconnectOptions is not null)
               {
                  grace = ClientDisconnectKind.Graceful;
               }

               var disconnectContext = new MqttDisconnectContext
               {
                  ServerClient = client,
                  Reason = disconnectOptions?.ReasonCode ?? DisconnectReasonCode.NormalDisconnection,
                  DisconnectKind = grace,
                  IsSessionTakenOver = false
               };

               await Events.OnDisconnect.ExecuteAsync(
                  disconnectContext, HandlerExecutionStrategy.SequentialContinueOnError, ct);
            }
         }
         catch (Exception)
         {
            /* ignored */
         }

         if (client is not null)
         {
            await ClientSessions.HandleClientDisconnectAsync(client);
            _serverClientPool.Return(client);
         }

         if (packetHandler is not null)
            _packetHandlerPool.Return(packetHandler);
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

         client.SetConnectOptions(connectOptions);
         var context = new MqttConnectInterceptContext(client)
         {
            CancellationToken = ct,
            ConnectOptions = connectOptions,
            NetworkSession = streamContext.Connection.Session
         };

         await Events.OnConnectIntercept.ExecuteAsync(
            context, HandlerExecutionStrategy.SequentialContinueOnError, ct);

         var assignedClientIdUtf8Bytes = ReadOnlyMemory<byte>.Empty;
         if (connectOptions.ClientIdUtf8Bytes.IsEmpty)
         {
            connectOptions.ClientIdUtf8Bytes = context.AssignedClientIdentifierUtf8Bytes;
            if (client.ProtocolVersion is MqttProtocolVersion.V50)
            {
               assignedClientIdUtf8Bytes = context.AssignedClientIdentifierUtf8Bytes;
            }
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
            AssignedClientIdentifierUtf8Bytes = new ReadOnlySequence<byte>(assignedClientIdUtf8Bytes),

            PropertiesBytes = new ReadOnlySequence<byte>(context.ResponseUserProperties.WrittenMemory),
            ServerReferenceUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(context.ServerReference)),
            ReasonStringUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(context.ReasonString)),
            AuthenticationMethodUtf8Bytes = new ReadOnlySequence<byte>(connectOptions.AuthenticationMethodUtf8Bytes),
            AuthenticationDataBytes = new ReadOnlySequence<byte>(context.ResponseAuthenticationData)
         };

         if (context.ReasonCode is not ConnectReasonCode.Success)
         {
            await streamContext.Stream.Send(in connAck, client.ProtocolVersion, ct);
            await streamContext.Connection.Session.DisposeAsync();
            return;
         }

         var sessionResult = await ClientSessions.GetOrCreateSession(client, connectOptions, ct);
         connAck.IsSessionPresent = sessionResult.IsSessionPresent;

         await streamContext.Stream.Send(in connAck, client.ProtocolVersion, ct);

         if (Events.OnConnect.Count > 0)
         {
            await Events.OnConnect.ExecuteAsync(new MqttConnectContext()
            {
               Client = client
            }, HandlerExecutionStrategy.SequentialContinueOnError, ct);
         }

         if (sessionResult is { IsSessionPresent: true, Session.OfflineQueueCount: > 0 })
         {
            _ = Task.Run(() => DeliverOfflineMessagesAsync(client, sessionResult.Session, ct), ct);
         }
      }
      catch (Exception)
      {
         try
         {
            await streamContext.Connection.Session.DisposeAsync();
         }
         catch
         {
            // ignored
         }
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
               var sequenceReader = new SequenceReader<byte>(buffer);
               var parser = new PacketParser(streamContext.Stream, packetHandler, client.ProtocolVersion);
               var valueTask = parser.TryDispatch(ref sequenceReader, out var parsedBytes, ct);

               var parseResult = valueTask.IsCompletedSuccessfully
                  ? valueTask.Result
                  : await valueTask.ConfigureAwait(false);

               if (parseResult.Failed || parseResult.Success is PacketDispatchResult.ProtocolError
                      or PacketDispatchResult.InvalidPacketType)
               {
                  // Protocol violation: exit the loop to drop the connection
                  TraceLogger.LogClientError(
                     "MqttServer: Protocol violation or parser error (Result: {0}). Exiting receive loop.",
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

   private static async Task DeliverOfflineMessagesAsync(MqttServerClient client, MqttSession session,
      CancellationToken ct)
   {
      try
      {
         var buffer = new byte[32];
         while (client.IsConnected && !ct.IsCancellationRequested)
         {
            if (!session.TryDequeueOfflineMessage(out var queuedMessage))
            {
               break;
            }

            var message = queuedMessage.Message;
            var targetQos = queuedMessage.QualityOfService;

            var topicBytes = Encoding.UTF8.GetBytes(message.Topic);
            var responseTopicBytes = string.IsNullOrEmpty(message.ResponseTopic)
               ? ReadOnlyMemory<byte>.Empty
               : Encoding.UTF8.GetBytes(message.ResponseTopic);

            var contentTypeBytes = string.IsNullOrEmpty(message.ContentType)
               ? ReadOnlyMemory<byte>.Empty
               : Encoding.UTF8.GetBytes(message.ContentType);

            var publishPacket = new PublishPacket
            {
               Dup = false,
               QualityOfService = targetQos,
               Retain = queuedMessage.RetainAsPublished && message.Retain,
               TopicUtf8Bytes = new ReadOnlySequence<byte>(topicBytes),
               Payload = new ReadOnlySequence<byte>(message.Payload),
               PacketIdentifier = targetQos > 0 ? session.GenerateNextPacketIdentifier() : (ushort)0,
               PayloadFormat = message.PayloadFormat,
               MessageExpiryInterval = message.MessageExpiryInterval,
               TopicAlias = 0,
               ResponseTopicUtf8Bytes = new ReadOnlySequence<byte>(responseTopicBytes),
               CorrelationDataBytes = message.CorrelationData.HasValue
                  ? new ReadOnlySequence<byte>(message.CorrelationData.Value)
                  : ReadOnlySequence<byte>.Empty,
               ContentTypeUtf8Bytes = new ReadOnlySequence<byte>(contentTypeBytes),
               PropertiesBytes = ReadOnlySequence<byte>.Empty
            };

            if (queuedMessage.SubscriptionIdentifier > 0 && client.ProtocolVersion is MqttProtocolVersion.V50)
            {
               var writer = new ByteWriter(buffer);
               var propEncoder = writer.AsPublishPropertyEncoder();

               propEncoder.WriteSubscriptionIdentifier(queuedMessage.SubscriptionIdentifier);

               var written = propEncoder.Encoder.Writer.Position;
               publishPacket.PropertiesBytes = new ReadOnlySequence<byte>(buffer.AsMemory(0, written));
            }

            await client.Stream.Send(in publishPacket, client.ProtocolVersion, ct);
         }
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("MqttServer: Error delivering offline messages to client '{0}': {1}",
            client.ClientIdUtf8Bytes.GetUtf8String(), ex.Message);
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

      SubscriptionRouter.Dispose();
   }
}
