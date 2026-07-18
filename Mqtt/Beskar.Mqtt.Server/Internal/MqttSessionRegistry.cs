using System.Diagnostics.CodeAnalysis;
using Beskar.Networking.Abstractions.Comparers;

namespace Beskar.Mqtt.Server.Internal;

public sealed class MqttSessionRegistry : IAsyncDisposable
{
   private readonly Dictionary<byte[], MqttSession> _sessions = new(2048, ByteArrayEqualityComparer.Instance);

   public MqttSession? Get(ReadOnlySpan<byte> clientIdUtf8Bytes, out MqttSession? existingSession)
   {
      existingSession = null;
      if (clientIdUtf8Bytes.IsEmpty) return null;

      var alternateLookup = _sessions.GetAlternateLookup<ReadOnlySpan<byte>>();
      if (!alternateLookup.TryGetValue(clientIdUtf8Bytes, out var session)) return null;

      if (session is { DisconnectionTimestamp: { } timestamp, ExpiryInterval: not uint.MaxValue }
          && timestamp.AddSeconds(session.ExpiryInterval) <= DateTimeOffset.UtcNow)
      {
         TryRemove(clientIdUtf8Bytes, out existingSession);
         return null;
      }

      return session;
   }

   public void Update(ReadOnlySpan<byte> clientIdUtf8Bytes, MqttSession session)
   {
      var alternateLookup = _sessions.GetAlternateLookup<ReadOnlySpan<byte>>();
      alternateLookup[clientIdUtf8Bytes] = session;
   }

   public bool TryRemove(ReadOnlySpan<byte> clientIdUtf8Bytes, [MaybeNullWhen(false)] out MqttSession session)
   {
      var alternateLookup = _sessions.GetAlternateLookup<ReadOnlySpan<byte>>();
      return alternateLookup.Remove(clientIdUtf8Bytes, out _, out session);
   }

   public List<MqttSession> RemoveAndGetExpiredSessions()
   {
      List<MqttSession>? expired = null;
      var now = DateTimeOffset.UtcNow;

      foreach (var session in _sessions.Values)
      {
         if (session is { DisconnectionTimestamp: { } timestamp, ExpiryInterval: not uint.MaxValue }
             && timestamp.AddSeconds(session.ExpiryInterval) <= now)
         {
            expired ??= [];
            expired.Add(session);
         }
      }

      if (expired is not null)
      {
         foreach (var session in expired)
         {
            _sessions.Remove(session.ClientIdUtf8Bytes);
         }
      }

      return expired ?? [];
   }

   public async Task ClearAsync()
   {
      await DisposeAsync();
   }

   public async ValueTask DisposeAsync()
   {
      foreach (var (_, value) in _sessions)
      {
         await value.DisposeAsync();
      }

      _sessions.Clear();
   }
}
