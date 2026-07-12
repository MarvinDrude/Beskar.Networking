using Beskar.Memory.Threading;
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
            if (existing is not null)
            {
               if (connectOptions.CleanSession)
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
      return new MqttSession(client)
      {
         ExpiryInterval = connectOptions.SessionExpiryInterval ?? 0,
      };
   }
}
