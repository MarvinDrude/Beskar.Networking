using System.Text;
using System.Text.Json;

namespace Beskar.Adv.Chat.Common;

public enum PacketType : byte
{
   Join = 1,
   Welcome = 2,
   Message = 3
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

public sealed class ChatPacket
{
   public PacketType Type { get; }
   public byte[] Payload { get; }

   public ChatPacket(PacketType type, byte[] payload)
   {
      Type = type;
      Payload = payload;
   }

   public string AsString() => Encoding.UTF8.GetString(Payload);

   public T? AsJson<T>()
   {
      try
      {
         return JsonSerializer.Deserialize<T>(Payload);
      }
      catch
      {
         return default;
      }
   }

   public static ChatPacket CreateJson<T>(PacketType type, T value)
   {
      var bytes = JsonSerializer.SerializeToUtf8Bytes(value);
      return new ChatPacket(type, bytes);
   }

   public static ChatPacket CreateString(PacketType type, string text)
   {
      var bytes = Encoding.UTF8.GetBytes(text);
      return new ChatPacket(type, bytes);
   }
}
