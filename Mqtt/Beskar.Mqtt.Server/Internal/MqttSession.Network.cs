using System.Diagnostics.CodeAnalysis;
using Beskar.Memory.Threading;
using Beskar.Mqtt.Server.Contexts;
using Beskar.Networking.Abstractions.Interfaces;

namespace Beskar.Mqtt.Server.Internal;

public sealed partial class MqttSession
{
   [MemberNotNullWhen(true,
      nameof(_serverClient))]
   public bool IsConnected { get; internal set; }

   private MqttServerClient? _serverClient;
   private volatile bool _disposed;

   public MqttServerClient? Client
   {
      get => _serverClient;
      internal set => _serverClient = value;
   }

   public byte[] ClientIdUtf8Bytes { get; }

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
       try
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
       catch (Exception)
       {
          throw;
       }
    }
}
