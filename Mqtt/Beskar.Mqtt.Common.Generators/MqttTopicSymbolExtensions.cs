using Microsoft.CodeAnalysis;

namespace Beskar.Mqtt.Common.Generators;

internal static class MqttTopicSymbolExtensions
{
   extension(ISymbol? symbol)
   {
      public bool IsGeneratedMqttTopicAttribute()
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
}
