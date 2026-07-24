using System.Buffers;
using System.Text.Json;
using Beskar.Networking.Protocol;

namespace Beskar.Resilient.Chat.Common;

public enum ChatPacketType
{
   Join,
   Welcome,
   Message
}

public sealed class ChatPacketEnvelope
{
   public ChatPacketType Type { get; set; }
   public string PayloadJson { get; set; } = string.Empty;

   public static ChatPacketEnvelope Create<T>(ChatPacketType type, T payload)
   {
      return new ChatPacketEnvelope
      {
         Type = type,
         PayloadJson = JsonSerializer.Serialize(payload)
      };
   }

   public T? GetPayload<T>()
   {
      return JsonSerializer.Deserialize<T>(PayloadJson);
   }
}

public sealed class ChatMessage
{
   public string Sender { get; set; } = string.Empty;
   public string Text { get; set; } = string.Empty;
   public DateTime Timestamp { get; set; }
}

public sealed class JoinPayload
{
   public string Username { get; set; } = string.Empty;
}

public sealed class WelcomePayload
{
   public string Username { get; set; } = string.Empty;
   public List<ChatMessage> History { get; set; } = [];
}

public sealed class ChatSerializer : IResilientSerializer
{
   public void Serialize<T>(T value, IBufferWriter<byte> writer)
   {
      using var jsonWriter = new Utf8JsonWriter(writer);
      JsonSerializer.Serialize(jsonWriter, value);
   }

   public T? Deserialize<T>(in ReadOnlySequence<byte> sequence)
   {
      var reader = new Utf8JsonReader(sequence);
      return JsonSerializer.Deserialize<T>(ref reader);
   }
}
