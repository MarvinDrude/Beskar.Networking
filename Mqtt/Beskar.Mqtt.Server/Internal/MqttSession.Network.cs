using System.Diagnostics.CodeAnalysis;
using Beskar.Memory.Threading;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttSession
{
   /// <summary>
   /// Indicates whether the MQTT session is currently connected to a client.
   /// </summary>
   /// <remarks>
   /// This property returns <c>true</c> if the session has an active client connection,
   /// otherwise it returns <c>false</c>. The connection status is managed internally
   /// and reflects the state of the associated <see cref="MqttServerClient"/> instance.
   /// </remarks>
   [MemberNotNullWhen(true,
      nameof(_serverClient))]
   public bool IsConnected { get; internal set; }

   private MqttServerClient? _serverClient;
   private volatile bool _disposed;

   /// <summary>
   /// Represents the client associated with the current MQTT session.
   /// </summary>
   /// <remarks>
   /// This property provides access to the <see cref="MqttServerClient"/> instance that is linked
   /// to the session. It may be <c>null</c> if the session has no client currently connected or
   /// associated. Modifications to this property are managed internally by the MQTT server framework.
   /// </remarks>
   public MqttServerClient? Client
   {
      get => _serverClient;
      internal set => _serverClient = value;
   }

   internal MqttSession(
      MqttServer server,
      MqttServerClient? serverClient)
   {
      Server = server;
      _serverClient = serverClient;

      ClientIdUtf8Bytes = serverClient is not null
         ? serverClient.ClientIdUtf8Bytes.ToArray()
         : [];

      IsConnected = serverClient is not null;
   }

    public async ValueTask DisposeAsync()
    {
       if (_disposed) return;
       _disposed = true;

       if (PendingWillMessage is not null)
       {
          if (PendingWillMessage.WillDelayInterval == 0 || ExpiryInterval > 0)
          {
             PendingWillMessage.TryPublish(Server, Server.ClientSessions);
          }
       }

       if (Server.Events.OnDeleteSession.Count > 0)
       {
          await Server.Events.OnDeleteSession.ExecuteAsync(new MqttDeleteSessionContext() { Session = this },
             HandlerExecutionStrategy.SequentialContinueOnError);
       }

       Server.SubscriptionRouter.UnsubscribeAll(this);

       lock (_incomingQos2Packets)
       {
          _incomingQos2Packets.Clear();
       }
       lock (_offlineQueue)
       {
          _offlineQueue.Clear();
       }

       _serverClient = null;
    }
}
