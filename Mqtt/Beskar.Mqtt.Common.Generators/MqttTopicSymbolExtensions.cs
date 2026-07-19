using Microsoft.CodeAnalysis;

namespace Beskar.Mqtt.Common.Generators;

internal static class MqttTopicSymbolExtensions
{
   public static bool IsGeneratedMqttTopicAttribute(this ISymbol? symbol)
   {
      return symbol is
      {
         Name: "GeneratedMqttTopicAttribute",
         ContainingNamespace:
         {
            Name: "Generators",
            ContainingNamespace:
            {
               Name: "Common",
               ContainingNamespace:
               {
                  Name: "Mqtt",
                  ContainingNamespace:
                  {
                     Name: "Beskar",
                     ContainingNamespace.IsGlobalNamespace: true
                  }
               }
            }
         }
      };
   }
}
