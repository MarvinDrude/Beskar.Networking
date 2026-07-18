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
using Beskar.Mqtt.Protocol.Collections;
using Beskar.Utilities.Tracing;
using Beskar.Mqtt.Protocol.Extensions;
using Beskar.Mqtt.Protocol.Models;

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

   public IReadOnlyList<INetworkListener> Listeners => _listeners;

   public MqttRetainedMessages RetainedMessages { get; } = new();
   internal MqttTrieSubscriptionRouter SubscriptionRouter { get; } = new();
   internal MqttClientSessions ClientSessions { get; }
   internal MqttServerOptions Options { get; }

   private volatile bool _disposed;
   private volatile int _state = (int)MqttServerState.Stopped;

   private readonly INetworkListener[] _listeners;
   private CancellationTokenSource _cancellationTokenSource = new();

   private readonly MqttKeepAliveService _keepAliveService;

   internal MqttServer(INetworkListener[] listeners, MqttServerOptions options)
   {
      _listeners = listeners;
      Options = options;

      ClientSessions = new MqttClientSessions(this);
      _keepAliveService = new MqttKeepAliveService(this);
   }

   public async Task<VoidResult<StringError>> StartAsync()
   {
      if (_disposed)
         return new StringError("Already disposed server.");

      if (State is not MqttServerState.Stopped)
         return new StringError("Server is not in stopped state.");

      State = MqttServerState.Starting;

      if (Events.OnLoadingRetainedMessages.Count > 0)
      {
         var context = new MqttLoadingRetainedMessagesContext { Server = this };
         await Events.OnLoadingRetainedMessages.ExecuteAsync(context, HandlerExecutionStrategy.SequentialContinueOnError);
         if (context.LoadedRetainedMessages.Count > 0)
         {
            RetainedMessages.LoadMessages(context.LoadedRetainedMessages);
         }
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

      _keepAliveService.Start();

      State = MqttServerState.Running;
      if (Events.OnStart.Count > 0)
      {
         await Events.OnStart.ExecuteAsync(new MqttServerStartContext() { Server = this },
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

      // notify clients

      foreach (var listener in _listeners)
      {
         await listener.UnbindAsync();
      }

      State = MqttServerState.Stopped;
      if (Events.OnStop.Count > 0)
      {
         await Events.OnStop.ExecuteAsync(new MqttServerStopContext() { Server = this },
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

         var connectionTask = Task.Factory.StartNew(
            () => RunClientConnectionTask(client, streamContext, packetHandler, combinedToken),
            TaskCreationOptions.PreferFairness).Unwrap();

         try
         {
            await RunClientListenTask(client, streamContext, packetHandler,
               async (_) =>
               {
                  await client.DisconnectAsync();
                  await session.DisposeAsync();
               }, combinedToken);
         }
         finally
         {
            try
            {
               await connectionTask;
            }
            catch
            {
               // ignored to prevent obscuring listener exceptions
            }
         }
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
            await client.DisconnectAsync();
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

         var propertiesBuffer = new byte[512];
         var connAckProperties = BuildConnAckProperties(
            this, client, context, connectOptions, assignedClientIdUtf8Bytes, propertiesBuffer);

         var connAck = new ConnAckPacket()
         {
            ResponseInfoUtf8Bytes = ReadOnlySequence<byte>.Empty,
            ReceiveMaximum = Options.ReceiveMaximum,
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

            PropertiesBytes = connAckProperties,
            ServerReferenceUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(context.ServerReference)),
            ReasonStringUtf8Bytes = new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(context.ReasonString)),
            AuthenticationMethodUtf8Bytes = new ReadOnlySequence<byte>(connectOptions.AuthenticationMethodUtf8Bytes),
            AuthenticationDataBytes = new ReadOnlySequence<byte>(context.ResponseAuthenticationData)
         };

         if (context.ReasonCode is not ConnectReasonCode.Success)
         {
            await streamContext.Stream.Send(in connAck, client.ProtocolVersion, ct);

            try
            {
               await streamContext.Stream.Transport.Output.CompleteAsync();
            }
            catch { /* ignored */ }

            await client.DisconnectAsync();
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

         if (sessionResult.IsSessionPresent
             && (sessionResult.Session.OfflineQueueCount > 0 || sessionResult.Session.HasUnacknowledgedPublishes))
         {
            _ = Task.Run(() => DeliverOfflineMessagesAsync(client, sessionResult.Session, ct), ct);
         }
      }
      catch (Exception)
      {
         try
         {
            await client.DisconnectAsync();
         }
         catch
         {
            // ignored
         }
      }
   }

   private static ReadOnlySequence<byte> BuildConnAckProperties(
      MqttServer server,
      MqttServerClient client,
      MqttConnectInterceptContext context,
      ConnectOptions connectOptions,
      ReadOnlyMemory<byte> assignedClientIdBytes,
      byte[] propertiesBuffer)
   {
      var writer = new ByteWriter(propertiesBuffer);
      try
      {
         var propEncoder = writer.AsConnAckPropertyEncoder();
         try
         {
            if (server.Options.ReceiveMaximum > 0)
            {
               propEncoder.WriteReceiveMaximum(server.Options.ReceiveMaximum);
            }

            propEncoder.WriteMaximumQoS(QualityOfServiceType.ExactlyOnce);
            propEncoder.WriteRetainAvailable(true);
            propEncoder.WriteTopicAliasMaximum(ushort.MaxValue);
            propEncoder.WriteWildcardSubscriptionAvailable(true);
            propEncoder.WriteSubscriptionIdentifiersAvailable(true);
            propEncoder.WriteSharedSubscriptionAvailable(false);

            if (!assignedClientIdBytes.IsEmpty)
            {
               propEncoder.WriteAssignedClientIdentifier(assignedClientIdBytes.Span);
            }

            if (!string.IsNullOrEmpty(context.ReasonString))
            {
               propEncoder.WriteReasonString(Encoding.UTF8.GetBytes(context.ReasonString));
            }

            if (!string.IsNullOrEmpty(context.ServerReference))
            {
               propEncoder.WriteServerReference(Encoding.UTF8.GetBytes(context.ServerReference));
            }

            if (!connectOptions.AuthenticationMethodUtf8Bytes.IsEmpty)
            {
               propEncoder.WriteAuthenticationMethod(connectOptions.AuthenticationMethodUtf8Bytes.Span);
            }

            if (!context.ResponseAuthenticationData.IsEmpty)
            {
               propEncoder.WriteAuthenticationData(context.ResponseAuthenticationData.Span);
            }

            if (context.ResponseUserProperties.Count > 0)
            {
               var enumerator = context.ResponseUserProperties.GetEnumerator();
               while (enumerator.MoveNext())
               {
                  var prop = enumerator.Current;
                  propEncoder.WriteUserProperty(prop.KeyUtf8Bytes, prop.ValueBytes);
               }
            }
         }
         finally
         {
            writer = propEncoder.Encoder.Writer;
         }

         var written = writer.Position;
         if (written > 0)
         {
            return new ReadOnlySequence<byte>(propertiesBuffer.AsMemory(0, written));
         }
      }
      finally
      {
         writer.Dispose();
      }

      return ReadOnlySequence<byte>.Empty;
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

               var parseResult = await valueTask.ConfigureAwait(false);

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

   internal async Task PublishWillMessageAsync(
      string clientId,
      string topic,
      byte[] payload,
      QualityOfServiceType qos,
      bool retain,
      uint messageExpiryInterval,
      PayloadFormat payloadFormat,
      string? contentType,
      string? responseTopic,
      byte[]? correlationData,
      UserPropertyCollection userProperties)
   {
      var topicBytes = Encoding.UTF8.GetBytes(topic);
      var topicSequence = new ReadOnlySequence<byte>(topicBytes);

      var packet = new PublishPacket
      {
         Dup = false,
         QualityOfService = qos,
         Retain = retain,
         TopicUtf8Bytes = topicSequence,
         Payload = new ReadOnlySequence<byte>(payload),
         MessageExpiryInterval = messageExpiryInterval,
         PayloadFormat = payloadFormat,
         ContentTypeUtf8Bytes = string.IsNullOrEmpty(contentType)
            ? ReadOnlySequence<byte>.Empty : new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(contentType)),
         ResponseTopicUtf8Bytes = string.IsNullOrEmpty(responseTopic)
            ? ReadOnlySequence<byte>.Empty : new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(responseTopic)),
         CorrelationDataBytes = correlationData == null
            ? ReadOnlySequence<byte>.Empty : new ReadOnlySequence<byte>(correlationData)
      };

      var msg = new MqttPublishMessage(packet);

      if (retain)
      {
         var changed = RetainedMessages.UpdateMessage(clientId, msg);
         if (changed && Events.OnRetainedMessageChanged.Count > 0)
         {
            var stored = RetainedMessages.GetMessages();
            _ = Task.Run(async () =>
            {
               try
               {
                  await Events.OnRetainedMessageChanged.ExecuteAsync(new MqttRetainedMessageChangedContext
                  {
                     ClientId = clientId,
                     ChangedRetainedMessage = msg.Payload.IsEmpty ? null : msg,
                     StoredRetainedMessages = stored
                  }, HandlerExecutionStrategy.SequentialContinueOnError);
               }
               catch (Exception)
               {
                  /* ignored */
               }
            });
         }
      }

      var visitor = new ServerPacketHandler.PublishMessageDispatcherVisitor(null!, msg);
      SubscriptionRouter.Route(topicBytes, ref visitor);
   }

   private static async Task SendPublishMessageAsync(
      MqttServerClient client,
      MqttPublishMessage message,
      QualityOfServiceType qos,
      bool retainAsPublished,
      uint subscriptionIdentifier,
      ushort packetIdentifier,
      bool dup,
      byte[] propertiesBuffer,
      CancellationToken ct)
   {
      var remainingExpiry = message.MessageExpiryInterval;
      if (message.MessageExpiryInterval > 0)
      {
         var timeSpent = (uint)(DateTimeOffset.UtcNow - message.CreatedAt).TotalSeconds;
         if (timeSpent >= message.MessageExpiryInterval)
         {
            return; // Message expired, do not deliver
         }
         remainingExpiry = message.MessageExpiryInterval - timeSpent;
      }

      var topicBytes = Encoding.UTF8.GetBytes(message.Topic);
      var responseTopicBytes = string.IsNullOrEmpty(message.ResponseTopic)
         ? ReadOnlyMemory<byte>.Empty
         : Encoding.UTF8.GetBytes(message.ResponseTopic);

      var contentTypeBytes = string.IsNullOrEmpty(message.ContentType)
         ? ReadOnlyMemory<byte>.Empty
         : Encoding.UTF8.GetBytes(message.ContentType);

      var publishPacket = new PublishPacket
      {
         Dup = dup,
         QualityOfService = qos,
         Retain = retainAsPublished && message.Retain,
         TopicUtf8Bytes = new ReadOnlySequence<byte>(topicBytes),
         Payload = new ReadOnlySequence<byte>(message.Payload),
         PacketIdentifier = packetIdentifier,
         PayloadFormat = message.PayloadFormat,
         MessageExpiryInterval = remainingExpiry,
         TopicAlias = 0,
         ResponseTopicUtf8Bytes = new ReadOnlySequence<byte>(responseTopicBytes),
         CorrelationDataBytes = message.CorrelationData.HasValue
            ? new ReadOnlySequence<byte>(message.CorrelationData.Value)
            : ReadOnlySequence<byte>.Empty,
         ContentTypeUtf8Bytes = new ReadOnlySequence<byte>(contentTypeBytes),
         PropertiesBytes = ReadOnlySequence<byte>.Empty
      };

      if (client.ProtocolVersion is MqttProtocolVersion.V50)
      {
         var writer = new ByteWriter(propertiesBuffer);
         try
         {
            var propEncoder = writer.AsPublishPropertyEncoder();
            try
            {
               if (subscriptionIdentifier > 0)
               {
                  propEncoder.WriteSubscriptionIdentifier(subscriptionIdentifier);
               }

               if (message.PayloadFormat is not PayloadFormat.Unspecified)
               {
                  propEncoder.WritePayloadFormatIndicator(message.PayloadFormat);
               }

               if (remainingExpiry > 0)
               {
                  propEncoder.WriteMessageExpiryInterval(remainingExpiry);
               }

               if (!responseTopicBytes.IsEmpty)
               {
                  propEncoder.WriteResponseTopic(responseTopicBytes.Span);
               }

               if (message.CorrelationData.HasValue)
               {
                  propEncoder.WriteCorrelationData(message.CorrelationData.Value.Span);
               }

               if (!contentTypeBytes.IsEmpty)
               {
                  propEncoder.WriteContentType(contentTypeBytes.Span);
               }

               if (message.UserProperties.Count > 0)
               {
                  var enumerator = message.UserProperties.GetDirectEnumerator();
                  while (enumerator.MoveNext())
                  {
                     if (enumerator.Current.Identifier is not PropertyIdentifier.UserProperty)
                        continue;

                     var userProperty = enumerator.Current.AsUserProperty();
                     propEncoder.WriteUserProperty(userProperty.KeyBytes, userProperty.ValueBytes);
                  }
               }
            }
            finally
            {
               writer = propEncoder.Encoder.Writer;
            }

            var written = writer.Position;
            if (written > 0)
            {
               publishPacket.PropertiesBytes = new ReadOnlySequence<byte>(
                  propertiesBuffer.AsMemory(0, written));
            }
         }
         finally
         {
            writer.Dispose();
         }
      }

      await client.Stream.Send(in publishPacket, client.ProtocolVersion, ct);
   }

   private static async Task DeliverOfflineMessagesAsync(MqttServerClient client, MqttSession session,
      CancellationToken ct)
   {
      try
      {
         var propertiesBuffer = new byte[128];
         var unacknowledged = session.GetUnacknowledgedPublishes();

         foreach (var pending in unacknowledged)
         {
            if (!client.IsConnected || ct.IsCancellationRequested)
            {
               break;
            }

            if (pending.Message.MessageExpiryInterval > 0)
            {
               var timeSpent = (uint)(DateTimeOffset.UtcNow - pending.Message.CreatedAt).TotalSeconds;
               if (timeSpent >= pending.Message.MessageExpiryInterval)
               {
                  session.AcknowledgePublish(pending.PacketIdentifier);
                  continue;
               }
            }

            await SendPublishMessageAsync(
               client,
               pending.Message,
               pending.QualityOfService,
               pending.RetainAsPublished,
               pending.SubscriptionIdentifier,
               pending.PacketIdentifier,
               dup: true,
               propertiesBuffer,
               ct);
         }

         while (client.IsConnected && !ct.IsCancellationRequested)
         {
            if (session.GetUnacknowledgedPublishCount() >= session.ClientReceiveMaximum)
            {
               break;
            }

            if (!session.TryDequeueOfflineMessage(out var queuedMessage))
            {
               break;
            }

            if (queuedMessage.Message.MessageExpiryInterval > 0)
            {
               var timeSpent = (uint)(DateTimeOffset.UtcNow - queuedMessage.Message.CreatedAt).TotalSeconds;
               if (timeSpent >= queuedMessage.Message.MessageExpiryInterval)
               {
                  continue; // Message expired, discard and skip
               }
            }

            var targetQos = queuedMessage.QualityOfService;
            var packetId = targetQos > 0 ? session.GenerateNextPacketIdentifier() : (ushort)0;

            if (targetQos > 0)
            {
               session.AddUnacknowledgedPublish(new MqttPendingPublish
               {
                  PacketIdentifier = packetId,
                  Message = queuedMessage.Message,
                  QualityOfService = targetQos,
                  RetainAsPublished = queuedMessage.RetainAsPublished,
                  SubscriptionIdentifier = queuedMessage.SubscriptionIdentifier
               });
            }

            await SendPublishMessageAsync(
               client,
               queuedMessage.Message,
               targetQos,
               queuedMessage.RetainAsPublished,
               queuedMessage.SubscriptionIdentifier,
               packetId,
               dup: false,
               propertiesBuffer,
               ct);
         }
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("MqttServer: Error delivering offline messages to client '{0}': {1}",
            client.ClientIdUtf8Bytes.GetUtf8String(), ex.Message);
      }
   }

   internal static async Task DeliverNextQueuedMessagesAsync(MqttSession session)
   {
      var client = session.Client;
      if (client is null || !client.IsConnected) return;

      using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(client.CancellationToken);
      var ct = combinedCts.Token;

      try
      {
         var propertiesBuffer = new byte[128];
         while (client.IsConnected && !ct.IsCancellationRequested)
         {
            if (session.GetUnacknowledgedPublishes().Count >= session.ClientReceiveMaximum)
            {
               break;
            }

            if (!session.TryDequeueOfflineMessage(out var queuedMessage))
            {
               break;
            }

            if (queuedMessage.Message.MessageExpiryInterval > 0)
            {
               var timeSpent = (uint)(DateTimeOffset.UtcNow - queuedMessage.Message.CreatedAt).TotalSeconds;
               if (timeSpent >= queuedMessage.Message.MessageExpiryInterval)
               {
                  continue; // Message expired, discard and skip
               }
            }

            var targetQos = queuedMessage.QualityOfService;
            var packetId = targetQos > 0 ? session.GenerateNextPacketIdentifier() : (ushort)0;

            if (targetQos > 0)
            {
               session.AddUnacknowledgedPublish(new MqttPendingPublish
               {
                  PacketIdentifier = packetId,
                  Message = queuedMessage.Message,
                  QualityOfService = targetQos,
                  RetainAsPublished = queuedMessage.RetainAsPublished,
                  SubscriptionIdentifier = queuedMessage.SubscriptionIdentifier
               });
            }

            await SendPublishMessageAsync(
               client,
               queuedMessage.Message,
               targetQos,
               queuedMessage.RetainAsPublished,
               queuedMessage.SubscriptionIdentifier,
               packetId,
               dup: false,
               propertiesBuffer,
               ct);
         }
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("MqttServer: Error delivering next queued messages to client '{0}': {1}",
            client.ClientIdUtf8Bytes.GetUtf8String(), ex.Message);
      }
   }

   public async Task ClearRetainedMessagesAsync()
   {
      RetainedMessages.Clear();
      if (Events.OnRetainedMessagesCleared.Count > 0)
      {
         await Events.OnRetainedMessagesCleared.ExecuteAsync(
            new MqttRetainedMessagesClearedContext { Server = this },
            HandlerExecutionStrategy.SequentialContinueOnError);
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
      RetainedMessages.Dispose();
   }
}
