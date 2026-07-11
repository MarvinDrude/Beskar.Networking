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

      if (session.ExpiryInterval is <= 0 or uint.MaxValue
          || session.DisconnectionTimestamp is not { } timestamp
          || timestamp.AddSeconds(session.ExpiryInterval) <= DateTimeOffset.UtcNow)
      {
         return session;
      }

      TryRemove(clientIdUtf8Bytes, out existingSession);
      return null;
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
