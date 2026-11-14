using System.Buffers;
using System.Collections.Concurrent;
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
using Beskar.Networking.Abstractions.Errors;
using Beskar.Networking.Abstractions.Interfaces;
using Beskar.Networking.Abstractions.Models;
using Beskar.Mqtt.Common.Encoders.Properties;
using Beskar.Mqtt.Common.Encoders.Version3;
using Beskar.Mqtt.Common.Encoders.Version5;
using Beskar.Mqtt.Common.Telemetry;
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

   /// <summary>
   /// A read-only collection of network listeners associated with the MQTT server.
   /// Each listener handles incoming connections and communication over different network interfaces.
   /// </summary>
   public IReadOnlyList<INetworkListener> Listeners => _listeners;

   /// <summary>
   /// Manages retained messages within the MQTT server.
   /// Retained messages are stored on the server and sent to clients subscribing
   /// to topics that match the messages' topics, ensuring reliable message delivery
   /// even when clients connect after the messages are published.
   /// </summary>
   public MqttRetainedMessages RetainedMessages { get; } = new();

   internal MqttTrieSubscriptionRouter SubscriptionRouter { get; } = new();
   internal MqttClientSessions ClientSessions { get; }
   internal MqttServerOptions Options { get; }

   private int _disposedState; // 0 = active, 1 = disposed
   private int _activeConnectionCount;
   private volatile int _state = (int)MqttServerState.Stopped;

   private readonly INetworkListener[] _listeners;
   private CancellationTokenSource _cancellationTokenSource = new();

   private readonly MqttKeepAliveService _keepAliveService;
   private readonly ConcurrentDictionary<Task, byte> _activeClientTasks = new();

   internal MqttServer(INetworkListener[] listeners, MqttServerOptions options)
   {
      _listeners = listeners;
      Options = options;

      ClientSessions = new MqttClientSessions(this);
      _keepAliveService = new MqttKeepAliveService(this);
   }

   /// <summary>
   /// Starts the MQTT server asynchronously.
   /// This method transitions the server from a stopped state to a running state,
   /// initializes listeners, manages cancellation tokens, and triggers specified events.
   /// </summary>
   /// <returns>
   /// A task representing the asynchronous operation. The result will be a <see cref="VoidResult{TError}"/>
   /// indicating success or a <see cref="StringError"/> containing an error message if the operation fails.
   /// </returns>
   public async Task<VoidResult<StringError>> StartAsync()
   {
      if (Volatile.Read(ref _disposedState) == 1)
         return new StringError("Already disposed server.");

      if (State is not MqttServerState.Stopped)
         return new StringError("Server is not in stopped state.");

      State = MqttServerState.Starting;

      if (Events.OnLoadingRetainedMessages.Count > 0)
      {
         var context = new MqttLoadingRetainedMessagesContext { Server = this };
         await Events.OnLoadingRetainedMessages.ExecuteAsync(context,
            HandlerExecutionStrategy.SequentialContinueOnError);
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

         if (!startResult.Failed)
         {
            startedBuilder.Add(listener);
            continue;
         }

         await CleanupListenersAsync(startedBuilder, _cancellationTokenSource);
         State = MqttServerState.Stopped;
         return new StringError($"Failed to start one of the listener: {startResult.Error.Message}");
      }

      foreach (var listener in _listeners)
      {
         _ = Task.Run(() => RunAcceptTask(listener, ct), ct);
      }

      _keepAliveService.Start();

      State = MqttServerState.Running;
      if (Events.OnStart.Count > 0)
      {
         await Events.OnStart.ExecuteAsync(new MqttServerStartContext() { Server = this },
            HandlerExecutionStrategy.SequentialContinueOnError, ct);
      }

      return true;

      static async Task CleanupListenersAsync(ArrayBuilder<INetworkListener> builder, CancellationTokenSource cts)
      {
         try
         {
            await cts.CancelAsync();
         }
         catch (ObjectDisposedException)
         {
            // ignored
         }

         var cleanups = builder.WrittenSpan.ToArray();
         foreach (var cleanup in cleanups)
         {
            try
            {
               await cleanup.UnbindAsync(CancellationToken.None);
            }
            catch
            {
               // ignored
            }
         }
      }
   }

   /// <summary>
   /// Stops the MQTT server asynchronously.
   /// This method transitions the server from a running state to a stopped state,
   /// cleans up resources, notifies connected clients, and triggers specified events.
   /// </summary>
   /// <param name="options">
   /// The options specifying custom disconnect behavior for clients. If null, default options
   /// are used with a reason code indicating that the server is shutting down.
   /// </param>
   /// <returns>
   /// A task representing the asynchronous operation. The result will be a <see cref="VoidResult{StringError}"/>
   /// indicating success or a <see cref="StringError"/> containing an error message if the operation fails.
   /// </returns>
   public Task<VoidResult<StringError>> StopAsync(DisconnectOptions? options = null)
   {
      if (Volatile.Read(ref _disposedState) == 1)
         return Task.FromResult<VoidResult<StringError>>(new StringError("Already disposed server."));

      return StopCoreAsync(options);
   }

   private async Task<VoidResult<StringError>> StopCoreAsync(DisconnectOptions? options = null)
   {
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

      MqttServerClient[] activeClients;
      using (var clientsResult = await ClientSessions.GetClients())
      {
         activeClients = [.. clientsResult.WrittenSpan];
      }

      if (activeClients.Length > 0)
      {
         await Task.WhenAll(activeClients.Select(DisconnectClientAsync));
      }

      foreach (var listener in _listeners)
      {
         await listener.UnbindAsync();
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

      var pendingTasks = _activeClientTasks.Keys.ToArray();
      if (pendingTasks.Length > 0)
      {
         try
         {
            await Task.WhenAll(pendingTasks).WaitAsync(TimeSpan.FromSeconds(5));
         }
         catch (Exception)
         {
            // ignored
         }
      }

      State = MqttServerState.Stopped;
      if (Events.OnStop.Count > 0)
      {
         await Events.OnStop.ExecuteAsync(new MqttServerStopContext() { Server = this },
            HandlerExecutionStrategy.SequentialContinueOnError);
      }

      return true;

      async Task DisconnectClientAsync(MqttServerClient client)
      {
         try
         {
            await client.DisconnectAsync(options);
         }
         catch (Exception)
         {
            // ignored
         }
      }
   }

   private void TrackClientTask(Task task)
   {
      _activeClientTasks.TryAdd(task, 0);
      _ = task.ContinueWith(
         static (completedTask, state) => ((ConcurrentDictionary<Task, byte>)state!).TryRemove(completedTask, out _),
         _activeClientTasks,
         CancellationToken.None,
         TaskContinuationOptions.ExecuteSynchronously,
         TaskScheduler.Default);
   }

   private async Task RunAcceptTask(INetworkListener listener, CancellationToken ct)
   {
      while (!ct.IsCancellationRequested)
      {
         try
         {
            var session = await listener.AcceptSessionAsync(ct);
            if (session.Failed) continue;

            TrackClientTask(Task.Run(() => RunClientTask(listener, session.Success, ct), CancellationToken.None));
         }
         catch (Exception)
         {
            // ignored
         }
      }
   }

   private async Task RunClientTask(INetworkListener listener, INetworkSession session, CancellationToken ct)
   {
      if (ct.IsCancellationRequested
          || State is MqttServerState.Stopping or MqttServerState.Stopped
          || !OpenToNewConnections
          || !TryAcquireConnectionSlot())
      {
         await session.DisposeAsync();
         return;
      }

      MqttServerClient? client = null;
      ServerPacketHandler? packetHandler = null;

      try
      {
         var controlStream = await AcceptControlStreamWithTimeoutAsync(session, ct);
         if (controlStream.Failed)
         {
            await session.DisposeAsync();
            return;
         }

         var connectionContext = new NetworkServerConnectionContext(listener, session);
         var streamContext = new NetworkServerStreamContext(connectionContext, controlStream.Success);

         client = new MqttServerClient();
         client.Initialize(streamContext, Options);

         using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, client.CancellationToken);
         var combinedToken = combinedCts.Token;

         packetHandler = new ServerPacketHandler();
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
            if (client is not null && client.TryMarkDisconnectEventFired() && Events.OnDisconnect.Count > 0)
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
         }

         try
         {
            await session.DisposeAsync();
         }
         catch
         {
            /* ignored */
         }

         Interlocked.Decrement(ref _activeConnectionCount);
      }
   }

   private bool TryAcquireConnectionSlot()
   {
      var maxConnections = Options.MaxConnections;
      if (maxConnections <= 0)
      {
         Interlocked.Increment(ref _activeConnectionCount);
         return true;
      }

      while (true)
      {
         var current = Volatile.Read(ref _activeConnectionCount);
         if (current >= maxConnections)
         {
            return false;
         }

         if (Interlocked.CompareExchange(ref _activeConnectionCount, current + 1, current) == current)
         {
            return true;
         }
      }
   }

   private async ValueTask<Result<INetworkStream, NetworkCodeError>> AcceptControlStreamWithTimeoutAsync(
      INetworkSession session,
      CancellationToken ct)
   {
      if (Options.DefaultTimeout <= TimeSpan.Zero)
      {
         return await session.AcceptStreamAsync(ct);
      }

      using var timeoutCts = new CancellationTokenSource(Options.DefaultTimeout);
      using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(ct, timeoutCts.Token);

      return await session.AcceptStreamAsync(linkedCts.Token);
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
            MaximumPacketSize = Options.MaximumPacketSize,
            MaximumQualityOfService = QualityOfServiceType.ExactlyOnce,

            ReturnCode = context.ReasonCode.ToReturnCode,
            ReasonCode = context.ReasonCode,
            TopicAliasMaximum = Options.TopicAliasMaximum,
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
            catch
            {
               /* ignored */
            }

            await client.DisconnectAsync();
            return;
         }

         var sessionResult = await ClientSessions.GetOrCreateSession(client, connectOptions, ct);
         connAck.IsSessionPresent = sessionResult.IsSessionPresent;
         client.MarkHandshakeCompleted();

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

            if (server.Options.MaximumPacketSize > 0)
            {
               propEncoder.WriteMaximumPacketSize(server.Options.MaximumPacketSize);
            }

            propEncoder.WriteMaximumQoS(QualityOfServiceType.ExactlyOnce);
            propEncoder.WriteRetainAvailable(true);

            if (server.Options.TopicAliasMaximum > 0)
            {
               propEncoder.WriteTopicAliasMaximum(server.Options.TopicAliasMaximum);
            }

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

         if (writer.Position > 0)
         {
            return new ReadOnlySequence<byte>([.. writer.WrittenSpan]);
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
            var updatePacketTimestamp = client.UpdateLastPacketReceivedTimestamp;

            while (!buffer.IsEmpty && !ct.IsCancellationRequested && client is { IsConnected: true, IsFenced: false })
            {
               var sequenceReader = new SequenceReader<byte>(buffer);
               var parser = new PacketParser(
                  streamContext.Stream,
                  packetHandler,
                  client.ProtocolVersion,
                  Options.MaximumPacketSize,
                  updatePacketTimestamp);
               var parseResult = await parser.TryDispatch(ref sequenceReader, out var parsedBytes, ct).ConfigureAwait(false);

               if (parseResult.Failed || parseResult.Success is PacketDispatchResult.ProtocolError
                      or PacketDispatchResult.InvalidPacketType
                      or PacketDispatchResult.PacketTooLarge)
               {
                  if (parseResult.Success is PacketDispatchResult.PacketTooLarge
                      && client is { IsHandshakeCompleted: true, ProtocolVersion: MqttProtocolVersion.V50 })
                  {
                     await client.DisconnectAsync(new DisconnectOptions
                     {
                        ReasonCode = DisconnectReasonCode.PacketTooLarge
                     });
                  }

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
            if (result.IsCompleted) break;
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
      MqttMetrics.RecordLastWillTriggered();
      var topicBytes = Encoding.UTF8.GetBytes(topic);
      var topicSequence = new ReadOnlySequence<byte>(topicBytes);

      var propertiesBytes = ReadOnlySequence<byte>.Empty;
      if (userProperties.Count > 0)
      {
         Span<byte> propBuf = stackalloc byte[128];
         var writer = new ByteWriter(propBuf);
         try
         {
            var propEncoder = writer.AsPublishPropertyEncoder();
            try
            {
               var enumerator = userProperties.GetDirectEnumerator();
               while (enumerator.MoveNext())
               {
                  if (enumerator.Current.Identifier is not PropertyIdentifier.UserProperty)
                     continue;

                  var userProperty = enumerator.Current.AsUserProperty();
                  propEncoder.WriteUserProperty(userProperty.KeyBytes, userProperty.ValueBytes);
               }
            }
            finally
            {
               writer = propEncoder.Encoder.Writer;
            }

            if (writer.Position > 0)
            {
               propertiesBytes = new ReadOnlySequence<byte>([.. writer.WrittenSpan]);
            }
         }
         finally
         {
            writer.Dispose();
         }
      }

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
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(contentType)),
         ResponseTopicUtf8Bytes = string.IsNullOrEmpty(responseTopic)
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(Encoding.UTF8.GetBytes(responseTopic)),
         CorrelationDataBytes = correlationData == null
            ? ReadOnlySequence<byte>.Empty
            : new ReadOnlySequence<byte>(correlationData),
         PropertiesBytes = propertiesBytes
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

   /// <summary>
   /// Builds an outgoing PUBLISH packet if the message has not expired and fits within the client's Maximum Packet Size.
   /// </summary>
   internal static bool TryCreateOutgoingPublishPacket(
      MqttServerClient client,
      MqttPublishMessage message,
      QualityOfServiceType qos,
      bool retain,
      uint subscriptionIdentifier,
      ushort packetIdentifier,
      bool dup,
      Span<byte> propertiesScratchBuffer,
      out PublishPacket publishPacket)
   {
      var remainingExpiry = message.MessageExpiryInterval;
      if (message.MessageExpiryInterval > 0)
      {
         var timeSpent = (uint)(DateTimeOffset.UtcNow - message.CreatedAt).TotalSeconds;
         if (timeSpent >= message.MessageExpiryInterval)
         {
            publishPacket = default;
            return false; // Message expired, do not deliver
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

      publishPacket = new PublishPacket
      {
         Dup = dup,
         QualityOfService = qos,
         Retain = retain,
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
         var writer = new ByteWriter(propertiesScratchBuffer);
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

            if (writer.Position > 0)
            {
               publishPacket.PropertiesBytes = new ReadOnlySequence<byte>([.. writer.WrittenSpan]);
            }
         }
         finally
         {
            writer.Dispose();
         }

         if (client.ConnectOptions?.MaximumPacketSize is > 0 and var clientMaxV5Size &&
             PacketVersion5Encoder.CalculatePublishPacketLength(in publishPacket) > clientMaxV5Size)
         {
            publishPacket = default;
            return false; // MQTT §3.1.2.11.4: do not transmit packets exceeding client's MaximumPacketSize
         }
      }
      else if (client.ConnectOptions?.MaximumPacketSize is > 0 and var clientMaxV3Size &&
               PacketVersion3Encoder.CalculatePublishPacketLength(in publishPacket) > clientMaxV3Size)
      {
         publishPacket = default;
         return false;
      }

      return true;
   }

   private static async Task<bool> SendPublishMessageAsync(
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
      if (!TryCreateOutgoingPublishPacket(
             client,
             message,
             qos,
             retainAsPublished && message.Retain,
             subscriptionIdentifier,
             packetIdentifier,
             dup,
             propertiesBuffer,
             out var publishPacket))
      {
         return false;
      }

      await client.SendViaOutgoingQueueAsync(in publishPacket, ct);
      return true;
   }

   private static async Task DeliverOfflineMessagesAsync(MqttServerClient client, MqttSession session,
      CancellationToken ct)
   {
      var acquired = false;
      try
      {
         await session.DeliverySemaphore.WaitAsync(ct);
         acquired = true;

         var propertiesBuffer = new byte[128];
         var unacknowledged = session.GetUnacknowledgedPublishes();

         foreach (var pending in unacknowledged)
         {
            if (!client.IsConnected || ct.IsCancellationRequested)
            {
               break;
            }

            if (pending.IsPubRecReceived)
            {
               var pubRel = new PubRelPacket
               {
                  PacketIdentifier = pending.PacketIdentifier,
                  ReasonCode = PubRelReasonCode.Success
               };

               await client.SendViaOutgoingPubRelAsync(in pubRel, ct);
               continue;
            }

            if (!await SendPublishMessageAsync(
                   client,
                   pending.Message,
                   pending.QualityOfService,
                   pending.RetainAsPublished,
                   pending.SubscriptionIdentifier,
                   pending.PacketIdentifier,
                   dup: true,
                   propertiesBuffer,
                   ct))
            {
               session.AcknowledgePublish(pending.PacketIdentifier);
            }
         }

         await DrainOfflineQueueAsync(client, session, propertiesBuffer, ct);
      }
      catch (OperationCanceledException) when (ct.IsCancellationRequested)
      {
         // ignored
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("MqttServer: Error delivering offline messages to client '{0}': {1}",
            client.ClientIdUtf8Bytes.GetUtf8String(), ex.Message);
      }
      finally
      {
         if (acquired)
         {
            ReleaseDeliverySemaphoreAndScheduleNext(session, client);
         }
      }
   }

   internal static async Task DeliverNextQueuedMessagesAsync(MqttSession session)
   {
      var client = session.Client;
      if (client is null || !client.IsConnected) return;

      var initialCt = client.CancellationToken;
      var acquired = false;

      try
      {
         await session.DeliverySemaphore.WaitAsync(initialCt);
         acquired = true;

         client = session.Client;
         if (client is null || !client.IsConnected) return;

         using var combinedCts = CancellationTokenSource.CreateLinkedTokenSource(client.CancellationToken);
         var ct = combinedCts.Token;

         var propertiesBuffer = new byte[128];
         await DrainOfflineQueueAsync(client, session, propertiesBuffer, ct);
      }
      catch (OperationCanceledException) when (initialCt.IsCancellationRequested)
      {
         // ignored
      }
      catch (Exception ex)
      {
         TraceLogger.LogServerError("MqttServer: Error delivering next queued messages to client '{0}': {1}",
            client?.ClientIdUtf8Bytes.GetUtf8String() ?? Encoding.UTF8.GetString(session.ClientIdUtf8Bytes), ex.Message);
      }
      finally
      {
         if (acquired)
         {
            ReleaseDeliverySemaphoreAndScheduleNext(session, client);
         }
      }
   }

   internal static void ReleaseDeliverySemaphoreAndScheduleNext(MqttSession session, MqttServerClient? client)
   {
      bool shouldSchedule;
      lock (session.OutboundOrderLock)
      {
         session.DeliverySemaphore.Release();
         shouldSchedule = client is not null
                          && client.IsConnected
                          && client is { IsFenced: false, CancellationToken.IsCancellationRequested: false }
                          && session.IsActiveClient(client)
                          && session.OfflineQueueCount > 0
                          && session.GetUnacknowledgedPublishCount() < session.ClientReceiveMaximum;
      }

      if (shouldSchedule)
      {
         _ = Task.Run(() => DeliverNextQueuedMessagesAsync(session));
      }
   }

   private static async Task DrainOfflineQueueAsync(
      MqttServerClient client,
      MqttSession session,
      byte[] propertiesBuffer,
      CancellationToken ct)
   {
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

         var targetQos = queuedMessage.QualityOfService;
         var tentativeId = targetQos > 0 ? (ushort)1 : (ushort)0;

         if (!TryCreateOutgoingPublishPacket(
                client,
                queuedMessage.Message,
                targetQos,
                queuedMessage.RetainAsPublished && queuedMessage.Message.Retain,
                queuedMessage.SubscriptionIdentifier,
                tentativeId,
                dup: false,
                propertiesBuffer,
                out var publishPacket))
         {
            continue; // Expired or exceeds client MaximumPacketSize
         }

         if (targetQos > 0)
         {
            var packetId = session.GenerateNextPacketIdentifier();
            publishPacket.PacketIdentifier = packetId;

            var pendingPublish = new MqttPendingPublish
            {
               PacketIdentifier = packetId,
               Message = queuedMessage.Message,
               QualityOfService = targetQos,
               RetainAsPublished = queuedMessage.RetainAsPublished,
               SubscriptionIdentifier = queuedMessage.SubscriptionIdentifier
            };

            if (!session.TryAddUnacknowledgedPublish(pendingPublish))
            {
               session.EnqueueOfflineMessage(queuedMessage);
               break;
            }
         }

         await client.SendViaOutgoingQueueAsync(in publishPacket, ct);
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
      if (Interlocked.Exchange(ref _disposedState, 1) == 1) return;

      await StopCoreAsync();

      foreach (var listener in _listeners)
      {
         await listener.DisposeAsync();
      }

      await ClientSessions.DisposeAsync();

      SubscriptionRouter.Dispose();
      RetainedMessages.Dispose();
   }
}
