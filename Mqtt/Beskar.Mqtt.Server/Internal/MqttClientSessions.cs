using Beskar.Memory.Threading;
using Beskar.Memory.Writers;
using Beskar.Mqtt.Common.Builders.Connecting;
using Beskar.Mqtt.Common.Builders.Disconnecting;
using Beskar.Mqtt.Protocol.Enums;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Mqtt.Server.Enums;
using Beskar.Mqtt.Server.Results;
using Beskar.Networking.Abstractions.Comparers;
using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Mqtt.Server.Internal;

public sealed class MqttClientSessions(MqttServer server)
{
   private readonly MqttServer _server = server;

   private readonly AsyncLock _initiateLock = new();
   private readonly AsyncLock _clientLock = new();
   private readonly ReadWriteLock _modificationLock = new();

   private readonly Dictionary<byte[], MqttServerClient> _clients = new(2048, ByteArrayEqualityComparer.Instance);
   private readonly MqttSessionRegistry _sessions = new();

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

      using (await _initiateLock.LockAsync(ct))
      {
         MqttSession? existing;
         MqttSession? previousSession;
         MqttServerClient? takenOverClient;

         using (_modificationLock.EnterWriteLock(ct))
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

            using (await _clientLock.LockAsync(ct))
            {
               var alternateLookup = _clients.GetAlternateLookup<ReadOnlySpan<byte>>();
               if (alternateLookup.TryGetValue(serverClient.ClientIdUtf8Bytes.Span, out takenOverClient))
               {
                  hasTakeOver = true;
               }

               alternateLookup[serverClient.ClientIdUtf8Bytes.Span] = serverClient;
            }
         }

         if (!isSessionPresent && _server.Events.OnNewSession.Count > 0)
         {
            await _server.Events.OnNewSession.ExecuteAsync(new MqttNewSessionContext()
            {
               Session = session
            }, HandlerExecutionStrategy.SequentialContinueOnError, cancellationToken: ct);
         }

         if (takenOverClient is not null)
         {
            await takenOverClient.DisconnectAsync(new DisconnectOptions()
            {
               ReasonCode = DisconnectReasonCode.SessionTakenOver,
            });

            if (_server.Events.OnDisconnect.Count > 0)
            {
               await _server.Events.OnDisconnect.ExecuteAsync(new MqttDisconnectContext()
               {
                  Reason = DisconnectReasonCode.SessionTakenOver,
                  ServerClient = takenOverClient,
                  DisconnectKind = ClientDisconnectKind.Graceful,
                  IsSessionTakenOver = true
               }, HandlerExecutionStrategy.SequentialContinueOnError, cancellationToken: ct);
            }
         }

         if (previousSession is not null)
         {
            // dispose previous session if expired
            await previousSession.DisposeAsync();
         }

         if (existing is not null)
         {
            // dispose existing session if new one required
            await existing.DisposeAsync();
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
      };
   }

   public async Task HandleClientDisconnectAsync(MqttServerClient client)
   {
      var session = client.MqttSession;
      if (session is null) return;

      using (await _initiateLock.LockAsync())
      using (_modificationLock.EnterWriteLock())
      {
         using (await _clientLock.LockAsync())
         {
            var alternateLookup = _clients.GetAlternateLookup<ReadOnlySpan<byte>>();
            alternateLookup.Remove(client.ClientIdUtf8Bytes.Span);
         }

         session.DisconnectionTimestamp = DateTimeOffset.UtcNow;
         session.Client = null;

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
               catch (Exception)
               {
                  /* ignored */
               }
            });
         }
      }
   }

   public async Task RemoveSessionAsync(MqttSession session)
   {
      using (await _initiateLock.LockAsync())
      using (_modificationLock.EnterWriteLock())
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
            catch (Exception)
            {
               /* ignored */
            }
         });
      }
   }
}
