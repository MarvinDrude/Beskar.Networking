namespace Beskar.Adv.Chat.Common;

public static class NameGenerator
{
   private static readonly string[] Adjectives =
   [
      "Happy", "Silent", "Swift", "Clever", "Brave", "Jolly", "Cool", "Gentle", "Cunning", "Bold",
      "Fiery", "Frosty", "Shiny", "Misty", "Wild", "Calm", "Quick", "Smart", "Lazy", "Active"
   ];

   private static readonly string[] Nouns =
   [
      "Panda", "Koala", "Tiger", "Eagle", "Falcon", "Wolf", "Fox", "Bear", "Otter", "Badger",
      "Lynx", "Panther", "Leopard", "Dolphin", "Orca", "Shark", "Owl", "Hawk", "Raven", "Deer"
   ];

   private static readonly Random Rnd = new();

   public static string Generate()
   {
      var adj = Adjectives[Rnd.Next(Adjectives.Length)];
      var noun = Nouns[Rnd.Next(Nouns.Length)];
      var num = Rnd.Next(100, 1000);

      return $"{adj}{noun}_{num}";
   }
}
