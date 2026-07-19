using System.Buffers;
using System.Collections.Concurrent;
using System.Text;
using System.Threading.Channels;
using Beskar.Memory.Threading;
using Beskar.Memory.Writers;
using Beskar.Utilities.Tracing;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Collections;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Mqtt.Server.Enums;
using Beskar.Mqtt.Server.Results;
using Beskar.Networking.Abstractions.Comparers;
using Beskar.Networking.Abstractions.Threading;
using Beskar.Mqtt.Common.Handlers;

namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttClientSessions(MqttServer server)
{
   private readonly MqttServer _server = server;

   private readonly AsyncLock _initiateLock = new();
   private readonly AsyncLock _clientLock = new();

   private readonly Dictionary<byte[], MqttServerClient> _clients = new(2048, ByteArrayEqualityComparer.Instance);
   private readonly MqttSessionRegistry _sessions = new();

   private readonly ConcurrentDictionary<byte[], MqttWillMessageState> _pendingWillMessages = new(ByteArrayEqualityComparer.Instance);

   internal void RemovePendingWillMessage(byte[] clientId)
   {
      _pendingWillMessages.TryRemove(clientId, out _);
   }

   public async Task<ArrayBuilderResult<MqttServerClient>> GetClients()
   {
      using (await _clientLock.LockAsync())
      {
         var clients = new ArrayBuilder<MqttServerClient>(_clients.Count);
         foreach (var client in _clients.Values)
         {
            clients.Add(client);
         }

         return clients;
      }
   }

   public async Task<MqttSessionCreateResult> GetOrCreateSession(
      MqttServerClient serverClient,
      ConnectOptions connectOptions,
      CancellationToken ct)
   {
      MqttSession session;
      var isSessionPresent = false;
      var hasTakeOver = false;

      MqttServerClient? takenOverClient = null;
      MqttSession? existing = null;
      MqttSession? previousSession = null;

      using (await _initiateLock.LockAsync(ct))
      {
         existing = _sessions.Get(serverClient.ClientIdUtf8Bytes.Span, out previousSession);
         var cleanSession = connectOptions.CleanSession || !_server.Options.SupportPersistentSessions;

         if (existing is not null)
         {
            if (cleanSession)
            {
               session = InitializeNewSession(serverClient, connectOptions);
            }
            else
            {
               session = existing;
               existing = null;

               session.DisconnectionTimestamp = null;
               session.IsConnected = true;

               isSessionPresent = true;
               // any other session recovery
            }
         }
         else
         {
            session = InitializeNewSession(serverClient, connectOptions);
         }

         _sessions.Update(serverClient.ClientIdUtf8Bytes.Span, session);
         serverClient.MqttSession = session;
         session.Client = serverClient;

         var willAlternateLookup = _pendingWillMessages.GetAlternateLookup<ReadOnlySpan<byte>>();
         if (willAlternateLookup.TryRemove(serverClient.ClientIdUtf8Bytes.Span, out var oldWill))
         {
            oldWill.Cancel();
         }

         if (connectOptions.HasWill)
         {
            var willTopic = Encoding.UTF8.GetString(connectOptions.WillTopicUtf8Bytes.Span);
            var willState = new MqttWillMessageState(
               serverClient.ClientIdUtf8Bytes.ToArray(),
               willTopic,
               connectOptions.WillPayload.ToArray(),
               connectOptions.WillQualityOfService,
               connectOptions.WillRetain,
               connectOptions.WillMessageExpiryInterval ?? 0,
               connectOptions.WillPayloadFormatIndicator,
               connectOptions.WillContentTypeUtf8Bytes.IsEmpty
                  ? null : Encoding.UTF8.GetString(connectOptions.WillContentTypeUtf8Bytes.Span),
               connectOptions.WillResponseTopicUtf8Bytes.IsEmpty
                  ? null : Encoding.UTF8.GetString(connectOptions.WillResponseTopicUtf8Bytes.Span),
               connectOptions.WillCorrelationDataBytes.IsEmpty
                  ? null : connectOptions.WillCorrelationDataBytes.ToArray(),
               UserPropertyCollection.Create(connectOptions.WillUserProperties.WrittenMemory),
               connectOptions.WillDelayInterval ?? 0
            );

            willAlternateLookup[serverClient.ClientIdUtf8Bytes.Span] = willState;
            session.PendingWillMessage = willState;
         }
         else
         {
            session.PendingWillMessage = null;
         }

         using (await _clientLock.LockAsync(ct))
         {
            var alternateLookup = _clients.GetAlternateLookup<ReadOnlySpan<byte>>();
            if (alternateLookup.TryGetValue(serverClient.ClientIdUtf8Bytes.Span, out takenOverClient))
            {
               hasTakeOver = true;
            }

            alternateLookup[serverClient.ClientIdUtf8Bytes.Span] = serverClient;
         }

         if (!isSessionPresent && _server.Events.OnNewSession.Count > 0)
         {
            await _server.Events.OnNewSession.ExecuteAsync(new MqttNewSessionContext()
            {
               Session = session
            }, HandlerExecutionStrategy.SequentialContinueOnError, cancellationToken: ct);
         }
      }

      if (takenOverClient is not null)
      {
         try
         {
            await takenOverClient.DisconnectAsync(new DisconnectOptions()
            {
               ReasonCode = DisconnectReasonCode.SessionTakenOver,
            });
         }
         catch (Exception ex)
         {
            TraceLogger.LogServerWarning("MqttClientSessions: Failed to send session taken over disconnect packet to old client. Error: {0}", ex.Message);
         }

         if (_server.Events.OnDisconnect.Count > 0)
         {
            try
            {
               await _server.Events.OnDisconnect.ExecuteAsync(new MqttDisconnectContext()
               {
                  Reason = DisconnectReasonCode.SessionTakenOver,
                  ServerClient = takenOverClient,
                  DisconnectKind = ClientDisconnectKind.Graceful,
                  IsSessionTakenOver = true
               }, HandlerExecutionStrategy.SequentialContinueOnError);
            }
            catch (Exception ex)
            {
               TraceLogger.LogServerWarning("MqttClientSessions: Error executing OnDisconnect for taken over client. Error: {0}", ex.Message);
            }
         }
      }

      if (previousSession is not null)
      {
         try
         {
            await previousSession.DisposeAsync();
         }
         catch (Exception ex)
         {
            TraceLogger.LogServerWarning("MqttClientSessions: Error disposing previous session. Error: {0}", ex.Message);
         }
      }

      if (existing is not null)
      {
         try
         {
            await existing.DisposeAsync();
         }
         catch (Exception ex)
         {
            TraceLogger.LogServerWarning("MqttClientSessions: Error disposing existing session. Error: {0}", ex.Message);
         }
      }

      return new MqttSessionCreateResult()
      {
         Session = session,
         IsSessionTakenOver = hasTakeOver,
         IsSessionPresent = isSessionPresent
      };
   }

   private MqttSession InitializeNewSession(MqttServerClient client, ConnectOptions connectOptions)
   {
      return new MqttSession(_server, client)
      {
         ExpiryInterval = _server.Options.SupportPersistentSessions ? (connectOptions.SessionExpiryInterval ?? 0) : 0,
         ClientReceiveMaximum = connectOptions.ReceiveMaximum ?? 65535,
      };
   }

   public async Task HandleClientDisconnectAsync(MqttServerClient client)
   {
      var session = client.MqttSession;
      if (session is null) return;

      using (await _initiateLock.LockAsync())
      {
         using (await _clientLock.LockAsync())
         {
            var alternateLookup = _clients.GetAlternateLookup<ReadOnlySpan<byte>>();
            if (alternateLookup.TryGetValue(client.ClientIdUtf8Bytes.Span, out var activeClient) && activeClient == client)
            {
               alternateLookup.Remove(client.ClientIdUtf8Bytes.Span);
            }
         }

         if (session.Client != client)
         {
            // already taken over
            return;
         }

         if (client.DisconnectOptions is not null && _server.Options.SupportPersistentSessions)
         {
            if (client.DisconnectOptions.SessionExpiryInterval.HasValue)
            {
               var val = client.DisconnectOptions.SessionExpiryInterval.Value;
               if (session.ExpiryInterval > 0 || val == 0)
               {
                  session.ExpiryInterval = val;
               }
            }
         }

         session.DisconnectionTimestamp = DateTimeOffset.UtcNow;
         session.Client = null;
         session.IsConnected = false;

         if (session.PendingWillMessage is not null)
         {
            if (client.DisconnectOptions?.ReasonCode is not DisconnectReasonCode.NormalDisconnection)
            {
               session.PendingWillMessage.StartDelayTimer(_server, this);
            }
            else
            {
               session.PendingWillMessage.Cancel();

               var alternateLookup = _pendingWillMessages.GetAlternateLookup<ReadOnlySpan<byte>>();
               alternateLookup.TryRemove(client.ClientIdUtf8Bytes.Span, out _);

               session.PendingWillMessage = null;
            }
         }

         if (session.ExpiryInterval == 0)
         {
            _sessions.TryRemove(client.ClientIdUtf8Bytes.Span, out _);

            _ = Task.Run(async () =>
            {
               try
               {
                  _server.SubscriptionRouter.UnsubscribeAll(session);
                  await session.DisposeAsync();
               }
               catch (Exception ex)
               {
                  TraceLogger.LogServerWarning("MqttClientSessions: Error disposing session. Error: {0}", ex.Message);
               }
            });
         }
      }
   }

   public async Task RemoveSessionAsync(MqttSession session)
   {
      using (await _initiateLock.LockAsync())
      {
         using (await _clientLock.LockAsync())
         {
            var alternateLookup = _clients.GetAlternateLookup<ReadOnlySpan<byte>>();
            alternateLookup.Remove(session.ClientIdUtf8Bytes);
         }

         _sessions.TryRemove(session.ClientIdUtf8Bytes, out _);

         _ = Task.Run(async () =>
         {
            try
            {
               _server.SubscriptionRouter.UnsubscribeAll(session);
               await session.DisposeAsync();
            }
            catch (Exception ex)
            {
               TraceLogger.LogServerWarning("MqttClientSessions: Error disposing session. Error: {0}", ex.Message);
            }
         });
      }
   }

   public async Task CleanupExpiredSessionsAsync()
   {
      List<MqttSession> expiredSessions;

      using (await _initiateLock.LockAsync())
      {
         expiredSessions = _sessions.RemoveAndGetExpiredSessions();
      }

      if (expiredSessions.Count > 0)
      {
         foreach (var session in expiredSessions)
         {
            try
            {
               _server.SubscriptionRouter.UnsubscribeAll(session);
               await session.DisposeAsync();
            }
            catch (Exception ex)
            {
               TraceLogger.LogServerWarning("Failed to dispose expired session for client '{0}': {1}",
                  Encoding.UTF8.GetString(session.ClientIdUtf8Bytes), ex.Message);
            }
         }
      }
   }
}
