using Beskar.Networking.Abstractions.Threading;

namespace Beskar.Mqtt.Server.Internal;

public sealed class MqttClientSessions
{
   private readonly AsyncLock _initiateLock = new();
   private readonly ReadWriteLock _modificationLock = new();

   private readonly MqttSessionRegistry _sessions = new();

   public async Task<MqttSession> GetOrCreateSession(
      MqttServerClient serverClient,
      CancellationToken ct)
   {
      using (await _initiateLock.LockAsync(ct))
      {
         MqttSession existing;

         using (_modificationLock.EnterWriteLock(ct))
         {

         }
      }

      return null!;
   }
}
